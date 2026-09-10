use super::*;
use kr_kafka_producer::{
    actor::{ActorConfig, ProducerActor},
    client::ClientClock,
    config::Compression,
    connector::{ConnectTarget, Connected, Connector},
    engine::ProducerEngine,
};
use kr_runtime::{HostRuntime, RuntimeHandle};
use kr_runtime_io::network::MemoryStream;
use std::{
    future::{Ready, ready},
    time::{Duration, Instant},
};
struct NoConnect;
impl Connector for NoConnect {
    type Stream = MemoryStream;
    type ConnectFuture = Ready<Result<Connected<MemoryStream>, ConnectError>>;
    fn connect(&mut self, _: ConnectTarget) -> Self::ConnectFuture {
        ready(Err(ConnectError::TransportUnavailable))
    }
}
fn prepare() -> (HostRuntime, ProducerClient, ProducerActor<NoConnect>) {
    let runtime = HostRuntime::default();
    let config = ProducerConfig {
        compression: Compression::None,
        record_descriptors: 16,
        delivery_event_capacity: 16,
        release_event_capacity: 4,
        max_live_leases: 4,
        max_open_topics: 4,
        max_batches: 16,
        pending_records_per_topic: 16,
        max_submission_records: 16,
        mailbox_capacity: 4,
        ..Default::default()
    };
    let engine = ProducerEngine::new(config, None).unwrap();
    let (client, actor) = ProducerActor::new(
        RuntimeHandle::Host(runtime.handle()),
        engine,
        NoConnect,
        ClientClock::Host(runtime.control()),
        ActorConfig {
            sim_encode_cost: RuntimeDuration::ZERO,
            ..Default::default()
        },
    )
    .unwrap();
    (runtime, client, actor)
}

fn run(send: std::sync::mpsc::SyncSender<Startup>) -> Result<EngineStatus, HostError> {
    let (mut runtime, client, actor) = prepare();
    announce(
        send,
        &client,
        Calibration {
            sample_bytes: 0,
            elapsed: Duration::ZERO,
            encode_bytes_per_poll: 16 * 1024,
        },
        Backend::Readiness,
    );
    let output = runtime
        .block_on(actor)
        .unwrap()
        .map_err(|e| HostError::Actor(e.to_string()));
    runtime.finish().unwrap();
    output
}
#[test]
fn startup_error_and_startup_panic_are_terminal_without_a_client() {
    assert!(matches!(
        HostProducer::spawn_owner(|send| {
            let error = HostError::Connect(ConnectError::TransportUnavailable);
            send.send(Err(error.clone())).unwrap();
            Err(error)
        }),
        Err(HostError::Connect(ConnectError::TransportUnavailable))
    ));
    assert!(matches!(
        HostProducer::spawn_owner(|_| panic!("startup injection")),
        Err(HostError::Panicked)
    ));
}
#[test]
fn dropping_wrapper_requests_close_and_owner_thread_retires() {
    let host = HostProducer::spawn_owner(run).unwrap();
    let status = host.status.clone();
    let client = host.client();
    drop(host);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if !matches!(*status.lock().unwrap(), HostStatus::Running) {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(matches!(
        *status.lock().unwrap(),
        HostStatus::Finished(EngineStatus { closed: true, .. })
    ));
    assert!(client.open_topic("after-close").is_err());
}
#[test]
fn owner_panic_after_startup_fences_the_client_and_join_reports_failure() {
    let (go, wait) = sync_channel(1);
    let host = HostProducer::spawn_owner(move |send| {
        let (_runtime, client, _actor) = prepare();
        announce(
            send,
            &client,
            Calibration {
                sample_bytes: 0,
                elapsed: Duration::ZERO,
                encode_bytes_per_poll: 16 * 1024,
            },
            Backend::Readiness,
        );
        wait.recv().unwrap();
        panic!("owner injection")
    })
    .unwrap();
    let client = host.client();
    go.send(()).unwrap();
    assert!(matches!(host.join(), Err(HostError::Panicked)));
    assert!(client.open_topic("after-panic").is_err());
}
#[test]
fn abandoned_startup_receiver_closes_the_constructed_actor() {
    let (send, receive) = sync_channel(1);
    drop(receive);
    let worker = std::thread::spawn(move || run(send));
    assert!(worker.join().unwrap().unwrap().closed);
}

#[test]
fn adversarial_panic_payload_is_disposed_before_final_owner_status() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct PanicOnDrop(Arc<AtomicUsize>);
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            // The secondary payload also has an adversarial destructor. The
            // runtime deliberately forgets it instead of recursively dropping.
            std::panic::panic_any(PanicOnDrop(self.0.clone()));
        }
    }
    let startup_drops = Arc::new(AtomicUsize::new(0));
    let inject = startup_drops.clone();
    assert!(matches!(
        HostProducer::spawn_owner(move |_| std::panic::panic_any(PanicOnDrop(inject))),
        Err(HostError::Panicked)
    ));
    assert_eq!(startup_drops.load(Ordering::SeqCst), 1);

    let drops = Arc::new(AtomicUsize::new(0));
    let inject = drops.clone();
    let (go, wait) = sync_channel(1);
    let host = HostProducer::spawn_owner(move |send| {
        let (_runtime, client, _actor) = prepare();
        announce(
            send,
            &client,
            Calibration {
                sample_bytes: 0,
                elapsed: Duration::ZERO,
                encode_bytes_per_poll: 16 * 1024,
            },
            Backend::Readiness,
        );
        wait.recv().unwrap();
        std::panic::panic_any(PanicOnDrop(inject));
    })
    .unwrap();
    let status = host.status.clone();
    let client = host.client();
    go.send(()).unwrap();
    assert!(matches!(host.join(), Err(HostError::Panicked)));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        *status.lock().unwrap(),
        HostStatus::Failed(HostError::Panicked)
    ));
    assert!(client.open_topic("after-adversarial-panic").is_err());
}

#[test]
fn native_options_preserve_defaults_and_forward_mode_without_modeled_time() {
    let defaults = HostProducerOptions::default();
    assert_eq!(defaults.write_mode, WriteMode::Staging);
    assert!(!defaults.diagnostics);
    let calibration = Calibration {
        sample_bytes: 1,
        elapsed: Duration::from_micros(1),
        encode_bytes_per_poll: 12_345,
    };
    for write_mode in [WriteMode::Staging, WriteMode::Vectored] {
        let options = HostProducerOptions {
            write_mode,
            diagnostics: true,
        };
        let actor = options.actor_config(calibration);
        assert_eq!(actor.write_mode, write_mode);
        assert_eq!(actor.encode_bytes_per_poll, 12_345);
        assert_eq!(actor.sim_encode_cost, RuntimeDuration::ZERO);
    }
}
