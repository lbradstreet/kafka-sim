//! Observes real owner publication without draining or replacing event owners.
//! Compiled only into the explicit foreign-binding conformance artifact.
use super::*;
use std::sync::Condvar;
use std::time::Duration;

#[derive(Default)]
pub(super) struct Publications {
    counts: Mutex<[u64; 2]>,
    changed: Condvar,
}

impl Publications {
    pub(super) fn published(&self, event: Event) {
        let index = match event {
            Event::Delivery(_) => 0,
            Event::FlushDone { .. } => 1,
            _ => return,
        };
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        counts[index] = counts[index].saturating_add(1);
        self.changed.notify_all();
    }
}

impl ProducerClient {
    /// Test-only count of delivery (kind 1) or flush (kind 3) events actually
    /// published by the owner into the application queue. Does not consume them.
    pub fn test_publication_count(&self, kind: u32) -> u64 {
        let counts = self
            .shared
            .test_publications
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        match kind {
            1 => counts[0],
            3 => counts[1],
            _ => 0,
        }
    }

    /// Test-only host wait. Publication holds the predicate mutex while notifying,
    /// so a completion before waiter registration cannot lose its wakeup.
    pub fn test_wait_publication(&self, kind: u32, minimum: u64, timeout: Duration) -> bool {
        let index = match kind {
            1 => 0,
            3 => 1,
            _ => return false,
        };
        let publications = &self.shared.test_publications;
        let counts = publications
            .counts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let (counts, _) = publications
            .changed
            .wait_timeout_while(counts, timeout, |counts| counts[index] < minimum)
            .unwrap_or_else(|error| error.into_inner());
        counts[index] >= minimum
    }
}
