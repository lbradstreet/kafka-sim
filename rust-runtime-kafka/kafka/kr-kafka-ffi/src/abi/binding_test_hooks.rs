//! Explicitly opt-in test artifact. Production constructors and ABI stay intact.
//! The actor runs on a real HostRuntime thread with a paused injected connector.
use super::*;
mod calls;
pub(super) use calls::{CallHooks, after_call};
use kr_kafka_producer::{
    actor::{ActorConfig, ProducerActor},
    client::ClientClock,
    config::Compression,
    connector::{ConnectError, ConnectTarget, Connected, Connector},
    engine::ProducerEngine,
};
use kr_runtime::{HostRuntime, RuntimeHandle};
use kr_runtime_io::network::MemoryStream;
use std::{
    future::{Future, Ready, ready},
    sync::mpsc::sync_channel,
};

struct Unavailable;
impl Connector for Unavailable {
    type Stream = MemoryStream;
    type ConnectFuture = Ready<Result<Connected<MemoryStream>, ConnectError>>;
    fn connect(&mut self, _: ConnectTarget) -> Self::ConnectFuture {
        ready(Err(ConnectError::Timeout))
    }
}

/// # Safety
/// `out` is writable and does not alias another active ABI argument.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_test_producer_create(out: *mut *mut KrProducer) -> i32 {
    code(run(|| {
        // SAFETY: the test binding provides an independent writable pointer output.
        unsafe { memory::write(out, std::ptr::null_mut()) }?;
        let config = ProducerConfig {
            compression: Compression::None,
            codec_contexts: 1,
            record_descriptors: 8,
            delivery_event_capacity: 8,
            release_event_capacity: 4,
            max_live_leases: 4,
            max_batches: 8,
            max_open_topics: 4,
            pending_records_per_topic: 8,
            mailbox_capacity: 4,
            max_submission_records: 8,
            input_bytes: 64 * 1024,
            ..Default::default()
        };
        let thread_config = config.clone();
        let (send, receive) = sync_channel(1);
        let (start, wait) = sync_channel(1);
        let abort = std::sync::Arc::new(AtomicBool::new(false));
        let thread_abort = abort.clone();
        let owner = std::thread::spawn(move || {
            let mut runtime = HostRuntime::default();
            let engine = ProducerEngine::new(thread_config, None).unwrap();
            let (client, actor) = ProducerActor::new(
                RuntimeHandle::Host(runtime.handle()),
                engine,
                Unavailable,
                ClientClock::Host(runtime.control()),
                ActorConfig {
                    sim_encode_cost: RuntimeDuration::ZERO,
                    ..Default::default()
                },
            )
            .unwrap();
            if send.send(client.clone()).is_err() {
                let _ = client.close(RuntimeDuration::ZERO);
            }
            let _ = wait.recv();
            let mut actor = Some(Box::pin(actor));
            let result = runtime
                .block_on(std::future::poll_fn(|cx| {
                    if thread_abort.load(Ordering::Acquire) {
                        actor.take();
                        return std::task::Poll::Ready(None);
                    }
                    actor.as_mut().unwrap().as_mut().poll(cx).map(Some)
                }))
                .unwrap();
            drop(actor);
            if let Some(result) = result {
                assert!(result.is_ok(), "binding fixture owner failed: {result:?}");
            }
            runtime.finish().unwrap();
        });
        let client = receive.recv().map_err(|_| KR_ERR_FAILED)?;
        let mut producer = match KrProducer::new(client.clone(), None, config) {
            Ok(producer) => producer,
            Err(error) => {
                let _ = client.close(RuntimeDuration::ZERO);
                let _ = start.send(());
                let _ = owner.join();
                return Err(error);
            }
        };
        producer.test_owner = Some(owner);
        producer.test_abort = abort;
        producer.test_start = Mutex::new(Some(start));
        // SAFETY: checked output remains writable until the constructor returns.
        unsafe { memory::write(out, Box::into_raw(Box::new(producer))) }?;
        Ok(())
    }))
}

/// # Safety
/// Handle is a live producer returned by the test constructor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_test_resume(producer: *mut KrProducer) -> i32 {
    code(with!(producer, true, |p| {
        if let Some(start) = p
            .test_start
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            start.send(()).map_err(|_| KR_ERR_FAILED)?;
        }
        Ok(())
    }))
}

/// # Safety
/// A live test producer; metadata is processed by its real owner and cache.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_test_metadata(
    producer: *mut KrProducer,
    topic: u32,
    partitions: u32,
    identity: u32,
) -> i32 {
    use kr_kafka_producer::{
        control::{BrokerNode, MetadataPartition, MetadataTopic, MetadataUpdate},
        topic::PartitionMetadata,
        types::TopicId,
    };
    code(with!(producer, false, |p| {
        if partitions == 0 || partitions > p.config.max_batches || !(1..=255).contains(&identity) {
            return Err(KR_ERR_INVALID);
        }
        let current = p
            .client
            .metadata_topic(TopicHandle(topic))
            .map_err(client_error)?
            .ok_or(KR_ERR_INVALID)?;
        let update = MetadataUpdate {
            throttle_ms: 0,
            cluster_id: Some("binding-tests".into()),
            controller_id: 0,
            brokers: (0..3)
                .map(|id| BrokerNode {
                    id,
                    host: format!("broker-{id}"),
                    port: 9092,
                    rack: Some(format!("rack-{id}")),
                })
                .collect(),
            topics: vec![MetadataTopic {
                requested_index: 0,
                id: TopicId([identity as u8; 16]),
                name: Some(current.name),
                error_code: 0,
                partitions: (0..partitions)
                    .map(|index| MetadataPartition {
                        index: index as i32,
                        error_code: 0,
                        metadata: PartitionMetadata {
                            leader: (index % 3) as i32,
                            leader_epoch: 1,
                        },
                        replicas: vec![0, 1, 2],
                        isr: vec![0, 1],
                        offline: vec![2],
                    })
                    .collect(),
            }],
        };
        p.client
            .test_metadata(TopicHandle(topic), update)
            .map_err(client_error)
    }))
}
/// # Safety
/// Live test producer. Terminal publication completes asynchronously; query status.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kr_test_abort(producer: *mut KrProducer) -> i32 {
    code(with!(producer, true, |p| {
        p.test_abort.store(true, Ordering::Release);
        p.client.fail_runtime().map_err(client_error)?;
        if let Some(start) = p
            .test_start
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            start.send(()).map_err(|_| KR_ERR_FAILED)?;
        }
        Ok(())
    }))
}
