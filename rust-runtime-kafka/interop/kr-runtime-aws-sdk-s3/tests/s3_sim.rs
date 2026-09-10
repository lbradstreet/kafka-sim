//! The simulated S3 client under deterministic simulation.
//!
//! These tests compile only under `--cfg kr_runtime_sim`:
//!
//! ```text
//! RUSTFLAGS='--cfg kr_runtime_sim' cargo test -p kr-runtime-aws-sdk-s3
//! ```

#![cfg(kr_runtime_sim)]

use kr_runtime::{RuntimeConfig, SimRuntime};
use kr_runtime_aws_sdk_s3::operation::get_object::GetObjectError;
use kr_runtime_aws_sdk_s3::primitives::ByteStream;
use kr_runtime_aws_sdk_s3::server::SimServer;
use kr_runtime_aws_sdk_s3::{Client, Config};
use kr_runtime_io::network::{LinkConfig, LinkKey, LinkState, NetworkConfig, NodeId, SimNetwork};
use kr_runtime_tokio::net::{SimNetContext, TcpListener};
use std::net::{Ipv4Addr, SocketAddr};

const SERVER_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 1);
const CLIENT_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 2);
const ENDPOINT: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(SERVER_IP), 443);

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

fn contexts(runtime: &SimRuntime) -> (SimNetwork, SimNetContext, SimNetContext) {
    let network =
        SimNetwork::new(runtime.handle(), NetworkConfig::default()).expect("network builds");
    let server = SimNetContext::new(network.clone(), SERVER_IP);
    let client = SimNetContext::new(network.clone(), CLIENT_IP);
    (network, server, client)
}

#[test]
fn the_s3_client_round_trips_objects_deterministically_over_the_simulated_network() {
    kr_runtime::seed_sweep!(4, |seed| {
        let run = |config: &RuntimeConfig| {
            let mut runtime = SimRuntime::new(config.clone());
            let (_network, server_context, client_context) = contexts(&runtime);
            let _ambient = client_context.install();
            runtime
                .block_on(async {
                    let listener = server_context
                        .scope(TcpListener::bind(ENDPOINT))
                        .await
                        .expect("server binds");
                    kr_runtime_tokio::task::spawn(
                        SimServer::builder().with_bucket("logs").serve(listener),
                    );

                    let client =
                        Client::from_conf(Config::builder().endpoint_addr(ENDPOINT).build());

                    let put = client
                        .put_object()
                        .bucket("logs")
                        .key("day/1")
                        .body(ByteStream::from_static(b"alpha"))
                        .send()
                        .await
                        .expect("put day/1");
                    client
                        .put_object()
                        .bucket("logs")
                        .key("day/2")
                        .body(ByteStream::from_static(b"beta"))
                        .send()
                        .await
                        .expect("put day/2");
                    client
                        .put_object()
                        .bucket("logs")
                        .key("other/3")
                        .body(ByteStream::from_static(b"gamma"))
                        .send()
                        .await
                        .expect("put other/3");

                    let fetched = client
                        .get_object()
                        .bucket("logs")
                        .key("day/1")
                        .send()
                        .await
                        .expect("get day/1");
                    assert_eq!(fetched.e_tag(), put.e_tag());
                    let body = fetched.body.collect().await.expect("collect body").to_vec();
                    assert_eq!(body, b"alpha");

                    let head = client
                        .head_object()
                        .bucket("logs")
                        .key("day/2")
                        .send()
                        .await
                        .expect("head day/2");
                    assert_eq!(head.content_length(), Some(4));

                    let listing = client
                        .list_objects_v2()
                        .bucket("logs")
                        .prefix("day/")
                        .send()
                        .await
                        .expect("list day/");
                    let keys: Vec<&str> = listing
                        .contents()
                        .iter()
                        .filter_map(|object| object.key())
                        .collect();
                    assert_eq!(keys, ["day/1", "day/2"]);
                    assert_eq!(listing.key_count(), Some(2));

                    client
                        .delete_object()
                        .bucket("logs")
                        .key("day/1")
                        .send()
                        .await
                        .expect("delete day/1");
                    let missing = client
                        .get_object()
                        .bucket("logs")
                        .key("day/1")
                        .send()
                        .await
                        .expect_err("deleted key is gone");
                    let service_error = missing.into_service_error();
                    assert!(
                        matches!(service_error, GetObjectError::NoSuchKey(_)),
                        "expected NoSuchKey, got: {service_error:?}"
                    );
                })
                .expect("root completes");
            runtime.snapshot().determinism_checkpoint()
        };
        let config = swept_config(seed);
        assert_eq!(run(&config), run(&config));
    });
}

#[test]
fn a_partitioned_link_surfaces_as_an_sdk_dispatch_failure() {
    let mut runtime = SimRuntime::new(swept_config(0));
    let (network, server_context, client_context) = contexts(&runtime);
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
    let _ambient = client_context.install();
    runtime
        .block_on(async {
            let listener = server_context
                .scope(TcpListener::bind(ENDPOINT))
                .await
                .expect("server binds");
            kr_runtime_tokio::task::spawn(SimServer::builder().with_bucket("logs").serve(listener));

            let client = Client::from_conf(Config::builder().endpoint_addr(ENDPOINT).build());
            let error = client
                .put_object()
                .bucket("logs")
                .key("day/1")
                .body(ByteStream::from_static(b"alpha"))
                .send()
                .await
                .expect_err("the client cannot reach the server");
            assert!(
                error.as_service_error().is_none(),
                "a partition is a dispatch failure, not a service error: {error:?}"
            );
        })
        .expect("root completes");
}
