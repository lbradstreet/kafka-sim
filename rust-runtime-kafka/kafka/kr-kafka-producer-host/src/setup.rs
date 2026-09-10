//! Producer control-credit adapter for request-independent native setup.
use kr_kafka_client::connector::{ConnectError, SetupBudget};
use kr_kafka_producer::credit::{Claim, Resource, SharedCredits};
use std::sync::Arc;

/// Shares the producer's existing control authority; constructing the adapter
/// reserves nothing. The host calls it only when a connect future is first polled.
#[derive(Clone, Debug)]
pub struct ProducerSetupBudget(SharedCredits);
impl ProducerSetupBudget {
    pub fn new(credits: SharedCredits) -> Self {
        Self(credits)
    }
}
impl SetupBudget for ProducerSetupBudget {
    fn reserve(&self, bytes: usize) -> Result<Arc<dyn Send + Sync>, ConnectError> {
        let guard = self
            .0
            .reserve(&[Claim {
                resource: Resource::ControlReserve,
                amount: bytes,
                lane: 0,
            }])
            .map_err(|_| ConnectError::ResourceExhausted)?;
        Ok(Arc::new(guard))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kr_kafka_client::connector::DATA_SETUP_BYTES;

    #[test]
    fn setup_uses_existing_authority_until_last_passive_guard_releases() {
        let mut limits = [1; Resource::COUNT];
        limits[Resource::ControlReserve as usize] = 2 * DATA_SETUP_BYTES;
        let credits = SharedCredits::new(limits, 1).unwrap();
        let budget = ProducerSetupBudget::new(credits.clone());
        assert!(credits.is_empty());
        let first = budget.reserve(DATA_SETUP_BYTES).unwrap();
        let retained = first.clone();
        let second = budget.reserve(DATA_SETUP_BYTES).unwrap();
        let before = credits.snapshot();
        assert!(matches!(
            budget.reserve(1),
            Err(ConnectError::ResourceExhausted)
        ));
        assert_eq!(credits.snapshot(), before);
        drop(first);
        assert_eq!(
            credits.snapshot()[Resource::ControlReserve as usize].held,
            2 * DATA_SETUP_BYTES
        );
        drop(retained);
        assert_eq!(
            credits.snapshot()[Resource::ControlReserve as usize].held,
            DATA_SETUP_BYTES
        );
        drop(second);
        assert!(credits.is_empty());
    }
}
