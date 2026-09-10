//! Exact retained element backing after successful engine construction.
//! Nested owners and construction transients are separate.
use super::*;

pub(crate) fn configured_storage_bytes(
    config: &ProducerConfig,
    connections: usize,
    requests: usize,
    control_events: usize,
    events: usize,
) -> Option<(usize, usize)> {
    let arenas = Pool::<Batch>::configured_storage_bytes(config.max_batches as usize)?
        .checked_add(Pool::<ConnectionState>::configured_storage_bytes(
            connections,
        )?)?
        .checked_add(Pool::<RequestState>::configured_storage_bytes(requests)?)?;
    let orders = connections
        .checked_mul(7)?
        .checked_add(config.max_open_topics as usize)?
        .checked_add(4)?;
    let queues = orders
        .checked_mul(OrderQueue::slot_bytes())?
        .checked_add(control_events.checked_mul(size_of::<FlushFence>())?)?
        .checked_add((config.max_batches as usize).checked_mul(size_of::<TerminalRecords>())?)?
        .checked_add(EventQueue::configured_storage_bytes(events)?)?;
    Some((arenas, queues))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_engine_storage_equals_all_retained_arena_and_queue_backing() {
        let config = ProducerConfig {
            compression: Compression::None,
            max_batches: 7,
            max_open_topics: 3,
            brokers_max: 1,
            record_descriptors: 8,
            delivery_event_capacity: 8,
            pending_records_per_topic: 8,
            max_live_leases: 4,
            release_event_capacity: 4,
            ..ProducerConfig::default()
        };
        let engine = ProducerEngine::new(config.clone(), None).unwrap();
        let fixed = engine.validated.memory.fixed_metadata;
        let actual = engine.batches.storage_capacity_bytes()
            + engine.connections.storage_capacity_bytes()
            + engine.requests.storage_capacity_bytes();
        assert_eq!(actual, fixed.engine_object_pools);
        let control = engine.validated.credits[Resource::ControlEvents as usize];
        let events = config.delivery_event_capacity as usize
            + config.release_event_capacity as usize
            + control;
        let requested_events = EventQueue::configured_storage_bytes(events).unwrap();
        assert_eq!(engine.events.storage_capacity_bytes(), requested_events);
        let actual_queues = engine.orders.storage_capacity_bytes()
            + engine.flushes.capacity() * size_of::<FlushFence>()
            + engine.terminal_records.capacity() * size_of::<TerminalRecords>()
            + engine.events.storage_capacity_bytes();
        assert_eq!(actual_queues, fixed.engine_queues);
        assert!(configured_storage_bytes(&config, usize::MAX, 1, 1, 1).is_none());
    }
}
