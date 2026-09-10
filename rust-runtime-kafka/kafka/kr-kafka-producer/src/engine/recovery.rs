//! Incremental identity recovery, including old connection ownership barriers.
use super::*;
use crate::sequence::IdentityChange;

pub(super) struct IdentityRefresh {
    next_slot: usize,
    retire_done: bool,
    request_issued: bool,
}

impl ProducerEngine {
    /// Install a checked local epoch bump while retaining idle connections.
    /// All old request plans and possibly transmitted ledger entries must have
    /// settled. The ledger fences dispatch throughout incremental refinalization.
    fn install_local_epoch(&mut self, identity: ProducerIdentity) -> Result<()> {
        let ledger = self
            .ledger
            .as_mut()
            .ok_or(EngineError::InvalidState("missing identity"))?;
        if !self.requests.is_empty() || ledger.identity().next_epoch() != Some(identity) {
            return Err(EngineError::Ledger(LedgerError::NotQuiescent));
        }
        ledger.begin_identity_install(identity)?;
        self.identity_pending = true;
        Ok(())
    }
    /// Accepts the actual returned identity. Existing assignments are rewritten
    /// by maintenance work; dispatch stays fenced until that work completes.
    pub fn install_identity(&mut self, identity: ProducerIdentity) -> Result<()> {
        if !identity.is_valid() {
            return Err(EngineError::Ledger(LedgerError::InvalidIdentity));
        }
        if let Some(ledger) = &mut self.ledger {
            if !self.connections.is_empty() || !self.requests.is_empty() {
                return Err(EngineError::Ledger(LedgerError::NotQuiescent));
            }
            ledger.begin_identity_install(identity)?;
            self.identity_refresh = None;
            self.identity_pending = true;
        } else {
            self.ledger = Some(ProducerLedger::new(
                identity,
                self.config.max_batches as usize,
                self.config.max_in_flight_per_connection as usize,
            )?);
            // Queued partitions are registered lazily before first assignment,
            // so receiving the initial identity never scans the topology.
            self.identity_pending = false;
            self.scheduler_identity_changed();
        }
        Ok(())
    }
    pub(super) fn has_identity_work(&self) -> bool {
        self.ledger
            .as_ref()
            .is_some_and(ProducerLedger::has_identity_install_work)
            || self.identity_refresh.as_ref().is_some_and(|work| {
                !work.retire_done || (!work.request_issued && self.connections.is_empty())
            })
    }
    /// Returns false when only provider/control completion can make progress.
    pub(super) fn identity_step(&mut self) -> bool {
        if self
            .ledger
            .as_ref()
            .is_some_and(ProducerLedger::has_identity_install_work)
        {
            let result = self.apply_identity_step();
            if result.is_err() {
                self.fail(FailureReason::ProtocolViolation);
            }
            return true;
        }
        let Some(work) = &mut self.identity_refresh else {
            return false;
        };
        if !work.retire_done {
            if work.next_slot < self.connections.allocated_slots() {
                let index = work.next_slot;
                work.next_slot += 1;
                if let Some(key) = self.connections.key_at(index) {
                    self.retire_connection(ConnectionKey(key.packed()), RetireReason::Requested);
                }
            } else {
                work.retire_done = true;
                if self.request_identity_if_retired().is_err() {
                    self.fail(FailureReason::ResourceExhausted);
                }
            }
            return true;
        }
        if !work.request_issued && self.connections.is_empty() {
            if self.request_identity_if_retired().is_err() {
                self.fail(FailureReason::ResourceExhausted);
            }
            return true;
        }
        false
    }
    fn apply_identity_step(&mut self) -> Result<()> {
        let progress = self
            .ledger
            .as_mut()
            .expect("installation ledger")
            .install_identity_step()?;
        match progress.change {
            Some(IdentityChange::Refinalized(change)) => {
                self.batches
                    .get_mut(Slot::from_packed(change.after.batch))
                    .ok_or(EngineError::InvalidState(
                        "identity assignment has no batch",
                    ))?
                    .refinalize(kr_kafka_record::Identity {
                        producer_id: change.after.identity.producer_id,
                        producer_epoch: change.after.identity.epoch,
                        base_sequence: change.after.base_sequence.get(),
                    })?;
            }
            Some(IdentityChange::Terminal(terminal)) => self.finish_batch(terminal),
            None => {}
        }
        if progress.complete {
            self.identity_pending = false;
            self.partition_cleanup_identity_changed();
            self.scheduler_identity_changed();
        }
        Ok(())
    }
    pub(super) fn request_identity_if_retired(&mut self) -> Result<()> {
        if self
            .identity_refresh
            .as_ref()
            .is_some_and(|work| work.retire_done && !work.request_issued)
            && self.connections.is_empty()
            && self.requests.is_empty()
        {
            self.order(EngineOrder::InitProducerId {
                // Java's nontransactional exhaustion path requests a fresh ID.
                previous: None,
            })?;
            self.identity_refresh
                .as_mut()
                .expect("refresh work")
                .request_issued = true;
        }
        Ok(())
    }
    pub(super) fn refresh_recovery(&mut self, _now: RuntimeInstant) {
        let Some(ledger) = &self.ledger else {
            return;
        };
        if ledger.recovery_state() != RecoveryState::NeedsIdentity
            || self.identity_pending
            || ledger.stats().active_attempts != 0
            || !self.requests.is_empty()
        {
            return;
        }
        let previous = match self
            .ledger
            .as_mut()
            .expect("checked ledger")
            .begin_identity_refresh()
        {
            Ok(previous) => previous,
            Err(_) => return,
        };
        self.identity_pending = true;
        if let Some(identity) = previous.next_epoch() {
            if self.install_local_epoch(identity).is_err() {
                self.fail(FailureReason::ProtocolViolation);
            }
            return;
        }
        // Exhaustion follows Java's availability policy. A new PID does not
        // fence remote old-PID stragglers; terminal Unknown outcomes stay final.
        self.identity_refresh = Some(IdentityRefresh {
            next_slot: 0,
            retire_done: false,
            request_issued: false,
        });
    }
}

#[cfg(test)]
mod tests;
