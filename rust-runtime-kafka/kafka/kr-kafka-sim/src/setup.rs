//! Shared environment construction for the ordinary and foreign-client drivers.
use crate::ReplayManifest;
use kr_kafka_broker_model::{BrokerEndpoint, BrokerModel};
use kr_runtime::{RuntimeDuration, SimRuntime, rng::RandomStream};
use kr_runtime_io::{
    SimLatencyModel,
    network::{LinkConfig, NetworkConfig, SimNetwork},
};
use std::{cell::RefCell, rc::Rc};

pub(crate) fn network_and_model(
    runtime: &SimRuntime,
    manifest: &ReplayManifest,
) -> Result<(SimNetwork, Rc<RefCell<BrokerModel>>), String> {
    let network = SimNetwork::new_with_schedule_random(
        runtime.handle(),
        NetworkConfig {
            max_connections: manifest.network.connections,
            max_inflight_operations: manifest.network.operations,
            max_listeners: manifest.network.listeners,
            max_listener_backlog: manifest.network.backlog,
            directional_buffer_bytes: manifest.driver.pipe_bytes,
            max_operation_bytes: manifest.network.operation_bytes,
            max_outstanding_read_bytes: manifest.network.read_bytes,
            max_outstanding_write_bytes: manifest.network.write_bytes,
            max_scripted_faults: manifest.network.scripted_faults,
            max_link_overrides: manifest.network.link_overrides,
            max_blocked_links: manifest.network.blocked_links,
            default_link: LinkConfig {
                max_chunk_bytes: manifest.driver.chunk_bytes,
                latency: RuntimeDuration::from_nanos(manifest.driver.link_latency_ns),
                ..LinkConfig::default()
            },
            latency_model: SimLatencyModel::UniformJitterV1 {
                max_jitter: RuntimeDuration::from_nanos(manifest.driver.jitter_ns),
            },
        },
        runtime.random_source(RandomStream::Schedule),
    )
    .map_err(|e| e.to_string())?;
    let mut model = BrokerModel::new(manifest.model.config(manifest.produce_max_version))
        .map_err(|e| e.to_string())?;
    for broker in &manifest.brokers {
        model
            .add_broker(BrokerEndpoint {
                id: broker.id,
                host: broker.host.clone(),
                port: broker.port,
            })
            .map_err(|e| e.to_string())?;
    }
    for (index, topic) in manifest.topics.iter().enumerate() {
        if manifest.initially_absent_topics.contains(&(index as u32)) {
            continue;
        }
        model
            .create_topic_with_id(&topic.name, topic.id, &topic.leaders)
            .map_err(|e| e.to_string())?;
    }
    let model = Rc::new(RefCell::new(model));
    Ok((network, model))
}
