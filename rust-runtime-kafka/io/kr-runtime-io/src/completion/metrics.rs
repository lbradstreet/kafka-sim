//! Opt-in native completion timing. Local/simulated completions never use clocks.
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Fixed-size counters for one explicitly instrumented set of native operations.
/// Snapshots are coherent; enabling these diagnostics adds clock reads and a
/// short mutex acquisition at admission, publication, and terminal drain.
#[derive(Default)]
pub struct CompletionMetrics {
    state: Mutex<CompletionSnapshot>,
}

/// Publication means the provider installed terminal output. Drain means that
/// output was taken by its observer, or its abandoned cell began destruction.
/// The histogram counts both; each bucket is an inclusive power-of-two upper
/// bound in nanoseconds (bucket zero is zero, bucket 64 is u64::MAX).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionSnapshot {
    pub observed_operations: u64,
    pub published: u64,
    pub consumed: u64,
    pub abandoned: u64,
    pub unpublished_discarded: u64,
    pub pending: u64,
    pub peak_pending: u64,
    pub undrained: u64,
    pub peak_undrained: u64,
    pub drain_delay_ns: [u64; 65],
    pub max_drain_delay_ns: u64,
    pub overflowed: bool,
}
impl Default for CompletionSnapshot {
    fn default() -> Self {
        Self {
            observed_operations: 0,
            published: 0,
            consumed: 0,
            abandoned: 0,
            unpublished_discarded: 0,
            pending: 0,
            peak_pending: 0,
            undrained: 0,
            peak_undrained: 0,
            drain_delay_ns: [0; 65],
            max_drain_delay_ns: 0,
            overflowed: false,
        }
    }
}
impl CompletionMetrics {
    pub fn snapshot(&self) -> CompletionSnapshot {
        super::lock_unpoisoned(&self.state).clone()
    }
}
fn increment(value: &mut u64, overflowed: &mut bool) {
    if let Some(next) = value.checked_add(1) {
        *value = next;
    } else {
        *overflowed = true;
    }
}
impl CompletionSnapshot {
    fn update(&mut self, f: impl FnOnce(&mut Self, &mut bool)) {
        let mut overflowed = self.overflowed;
        f(self, &mut overflowed);
        self.overflowed = overflowed;
    }
    fn drain(&mut self, ns: u64, abandoned: bool) {
        self.update(|s, overflow| {
            if abandoned {
                increment(&mut s.abandoned, overflow);
            } else {
                increment(&mut s.consumed, overflow);
            }
            s.undrained -= 1;
            let bucket = if ns == 0 {
                0
            } else {
                (64 - (ns - 1).leading_zeros()).max(1) as usize
            };
            increment(&mut s.drain_delay_ns[bucket], overflow);
            s.max_drain_delay_ns = s.max_drain_delay_ns.max(ns);
        });
    }
}
pub(super) struct Observation {
    metrics: Arc<CompletionMetrics>,
    published: Option<Instant>,
}
impl Observation {
    pub(super) fn new(metrics: Arc<CompletionMetrics>) -> Self {
        super::lock_unpoisoned(&metrics.state).update(|s, overflow| {
            increment(&mut s.observed_operations, overflow);
            increment(&mut s.pending, overflow);
            s.peak_pending = s.peak_pending.max(s.pending);
        });
        Self {
            metrics,
            published: None,
        }
    }
    pub(super) fn publish(&mut self) {
        self.published = Some(Instant::now());
        super::lock_unpoisoned(&self.metrics.state).update(|s, overflow| {
            s.pending -= 1;
            increment(&mut s.published, overflow);
            increment(&mut s.undrained, overflow);
            s.peak_undrained = s.peak_undrained.max(s.undrained);
        });
    }
    pub(super) fn finish(self, abandoned: bool) {
        let elapsed = self.published.map(|at| at.elapsed().as_nanos());
        let mut state = super::lock_unpoisoned(&self.metrics.state);
        if let Some(ns) = elapsed {
            if ns > u128::from(u64::MAX) {
                state.overflowed = true;
            }
            state.drain(u64::try_from(ns).unwrap_or(u64::MAX), abandoned);
        } else {
            state.update(|s, overflow| {
                s.pending -= 1;
                increment(&mut s.unpublished_discarded, overflow);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histogram_and_overflow_are_explicit() {
        let mut state = CompletionSnapshot {
            undrained: 7,
            ..Default::default()
        };
        for ns in [0, 1, 2, 3, 4, 5, u64::MAX] {
            state.drain(ns, false);
        }
        assert_eq!(&state.drain_delay_ns[..4], &[1, 2, 2, 1]);
        assert_eq!(state.drain_delay_ns[64], 1);
        assert_eq!(state.consumed, 7);
        assert_eq!(state.undrained, 0);
        state.consumed = u64::MAX;
        state.undrained = 1;
        state.drain(0, false);
        assert!(state.overflowed);
    }
}
