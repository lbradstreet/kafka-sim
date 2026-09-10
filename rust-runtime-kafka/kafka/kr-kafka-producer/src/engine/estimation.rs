use super::*;
use crate::estimation::{DeadlineHeadroom, EncodeWork, RoundTripTime};

#[cfg(test)]
mod tests;

pub(super) struct HeadroomSweep {
    cursor: usize,
    end: usize,
    restart: bool,
}

impl ProducerEngine {
    /// Supplies elapsed runtime/model time for the immediately preceding encode
    /// invocation. Idle work is ignored, and every invocation is consumed once.
    ///
    /// # Errors
    /// Returns an error if the encoder's actual-work counters are inconsistent.
    pub fn observe_encode_elapsed(&mut self, elapsed: RuntimeDuration) -> Result<()> {
        let work = std::mem::take(&mut self.last_encode_work);
        if std::mem::take(&mut self.last_encode_aborted) {
            self.encoding_cost.reset_pending_seal();
            return Ok(());
        }
        let changed = self
            .encoding_cost
            .observe(work, elapsed)
            .map_err(|_| EngineError::InvalidState("inconsistent encode timing work"))?;
        if changed {
            self.invalidate_headroom();
        }
        Ok(())
    }

    #[must_use]
    pub const fn last_encode_work(&self) -> EncodeWork {
        self.last_encode_work
    }

    pub(super) fn headroom_for(&self, partition: TopicPartition) -> DeadlineHeadroom {
        let rtt = self
            .destination(partition)
            .and_then(|(broker, _)| self.brokers.get(&broker))
            .map_or_else(RoundTripTime::default, |broker| broker.round_trip);
        DeadlineHeadroom::new(
            self.encoding_cost,
            rtt,
            self.config.request_timeout,
            self.config.delivery_timeout,
        )
        .expect("validated timeout bounds")
    }

    pub(super) fn invalidate_headroom(&mut self) {
        if self.failed.is_some() || self.closed || self.batches.is_empty() {
            return;
        }
        if let Some(sweep) = &mut self.headroom_sweep {
            sweep.restart = true;
        } else {
            self.headroom_sweep = Some(HeadroomSweep {
                cursor: 0,
                end: self.batches.allocated_slots(),
                restart: false,
            });
        }
    }

    /// One raw slot visit, including holes. Updates behind a partial frontier
    /// request one additional sweep, without accumulating stale queue entries.
    pub(super) fn headroom_step(&mut self, now: RuntimeInstant) -> bool {
        let Some(mut sweep) = self.headroom_sweep.take() else {
            return false;
        };
        if self.failed.is_some() || self.closed || self.batches.is_empty() {
            return true;
        }
        if sweep.cursor == sweep.end {
            if sweep.restart {
                self.headroom_sweep = Some(HeadroomSweep {
                    cursor: 0,
                    end: self.batches.allocated_slots(),
                    restart: false,
                });
            }
            return true;
        }
        let index = sweep.cursor;
        sweep.cursor += 1;
        if let Some(key) = self.batches.key_at(index) {
            let partition = self.batches.get(key).expect("live pool slot").partition();
            let estimate = self.headroom_for(partition);
            if self
                .batches
                .get_mut(key)
                .expect("live pool slot")
                .update_deadline_headroom(estimate)
            {
                self.refresh_batch_deadline(key, now);
            }
        }
        self.headroom_sweep = Some(sweep);
        true
    }

    pub(super) fn abort_seal_timing(&mut self) {
        self.encoding_cost.reset_pending_seal();
        self.last_encode_aborted = true;
    }
}
