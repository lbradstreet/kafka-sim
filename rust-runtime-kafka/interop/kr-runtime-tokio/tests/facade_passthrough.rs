//! Without `--cfg kr_runtime_sim`, the facade is real tokio: the same component
//! the simulation tests drive runs here on tokio's own runtime and clock.

#![cfg(not(kr_runtime_sim))]

mod common;

use kr_runtime_tokio::time::Duration;

#[test]
fn the_facade_is_transparent_tokio_without_the_sim_cfg() {
    let runtime = kr_runtime_tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("tokio runtime builds");
    let (ticks, elapsed) = runtime.block_on(common::timed_ticks(3, Duration::from_millis(2)));
    assert_eq!(ticks, 3);
    assert!(
        elapsed >= Duration::from_millis(6),
        "tokio timers wait real time: {elapsed:?}"
    );
}

#[test]
fn the_net_facade_is_transparent_tokio_without_the_sim_cfg() {
    let runtime = kr_runtime_tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("tokio runtime builds");
    runtime.block_on(async {
        let listener = kr_runtime_tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind succeeds");
        let address = listener.local_addr().expect("bound address is known");
        let server = kr_runtime_tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept succeeds");
            common::echo_once(stream).await.expect("echo completes")
        });
        let client = kr_runtime_tokio::net::TcpStream::connect(address)
            .await
            .expect("connect succeeds");
        let response = common::request_response(client, b"real tokio")
            .await
            .expect("exchange completes");
        assert_eq!(response, b"real tokio");
        assert_eq!(server.await.expect("server joins"), b"real tokio");
    });
}
