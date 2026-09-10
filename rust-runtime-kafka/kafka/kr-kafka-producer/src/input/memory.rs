//! Exact retained registry element backing after successful construction.
//! Referenced allocations and construction transients are separate.
use super::*;

pub(crate) fn configured_storage_bytes(config: &ProducerConfig) -> Option<usize> {
    let leases = config.max_live_leases as usize;
    Pool::<LeaseSlot>::configured_storage_bytes(leases)?.checked_add(
        (config.release_event_capacity as usize).checked_mul(size_of::<EventEnvelope>())?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actual_storage(registry: &InputLeases) -> usize {
        let state = registry.state.lock().unwrap_or_else(|p| p.into_inner());
        state.slots.storage_capacity_bytes()
            + registry
                .released
                .queue
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .capacity()
                * size_of::<EventEnvelope>()
    }

    #[test]
    fn registry_metadata_capacity_survives_full_release_and_generation_reuse() {
        let config = ProducerConfig {
            max_live_leases: 3,
            release_event_capacity: 3,
            ..ProducerConfig::default()
        };
        let validated = config.validate().unwrap();
        let credits = SharedCredits::new(validated.credits, config.lanes).unwrap();
        let registry = InputLeases::new(&config, credits.clone()).unwrap();
        let storage = actual_storage(&registry);
        assert_eq!(storage, configured_storage_bytes(&config).unwrap());
        assert_eq!(
            configured_storage_bytes(&config),
            Some(validated.memory.fixed_metadata.input_registry)
        );
        for _ in 0..100 {
            let leases: Vec<_> = (0..3)
                .map(|_| registry.acquire(16, 0).unwrap().commit(16).unwrap())
                .collect();
            assert!(registry.acquire(16, 0).is_err());
            for lease in leases {
                registry.release(lease).unwrap();
            }
            for _ in 0..3 {
                assert!(registry.pop_released().is_some());
            }
            assert!(registry.pop_released().is_none());
            assert_eq!(actual_storage(&registry), storage);
            assert!(credits.is_empty());
        }
    }
}
