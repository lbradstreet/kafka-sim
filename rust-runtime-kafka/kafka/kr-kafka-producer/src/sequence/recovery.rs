//! Bounded producer-wide identity installation while transmission is fenced.
use super::*;
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Debug)]
pub(super) struct IdentityInstall {
    identity: ProducerIdentity,
    cursor: Option<TopicPartition>,
    current: Option<PartitionInstall>,
    pub(super) restart: bool,
}
#[derive(Debug)]
struct PartitionInstall {
    partition: TopicPartition,
    index: usize,
    next: Sequence,
}

/// One bounded identity-installation result. A cancellation accepted while the
/// global write fence is held can terminate an untransmitted entry; the owner
/// must publish it just like an ordinary ledger terminal result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentityChange {
    Refinalized(Refinalization),
    Terminal(TerminalBatch),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdentityProgress {
    /// At most one partition/entry/restart bookkeeping visit per step.
    pub work_items: usize,
    pub change: Option<IdentityChange>,
    pub complete: bool,
}
impl ProducerLedger {
    /// Starts installing a returned identity without visiting partitions. The
    /// ledger remains RefreshingIdentity until the last bounded step. During
    /// this phase assignments can carry the returned identity, but no write
    /// attempt or new assignment is allowed. The owner must apply each returned
    /// refinalization to its retained batch before taking another step.
    ///
    /// Cancellations may interleave; their untransmitted outcomes are returned
    /// as terminal changes and the surviving sequence layout is recomputed.
    pub fn begin_identity_install(&mut self, identity: ProducerIdentity) -> Result<()> {
        if !identity.is_valid() {
            return Err(LedgerError::InvalidIdentity);
        }
        if self.recovery != RecoveryState::RefreshingIdentity || self.identity_install.is_some() {
            return Err(LedgerError::RecoveryPending);
        }
        if identity == self.identity {
            return Err(LedgerError::IdentityUnchanged);
        }
        if identity.producer_id == self.identity.producer_id && identity.epoch < self.identity.epoch
        {
            return Err(LedgerError::InvalidIdentity);
        }
        self.identity_install = Some(IdentityInstall {
            identity,
            cursor: None,
            current: None,
            restart: false,
        });
        Ok(())
    }
    #[must_use]
    pub fn has_identity_install_work(&self) -> bool {
        self.identity_install.is_some()
    }
    /// Visits one live entry or a constant amount of cursor bookkeeping. A
    /// partition window contains at most five entries, including on removal.
    /// Empty partitions are skipped by the existing nonempty index; their next
    /// sequence resets lazily when first assigned under the new identity.
    pub fn install_identity_step(&mut self) -> Result<IdentityProgress> {
        let mut install = self
            .identity_install
            .take()
            .ok_or(LedgerError::RecoveryPending)?;
        let mut progress = IdentityProgress {
            work_items: 1,
            change: None,
            complete: false,
        };
        if install.restart {
            install.cursor = None;
            install.current = None;
            install.restart = false;
            self.identity_install = Some(install);
            return Ok(progress);
        }
        if install.current.is_none() {
            let next = match install.cursor {
                Some(cursor) => self
                    .nonempty
                    .range((Excluded(cursor), Unbounded))
                    .next()
                    .copied(),
                None => self.nonempty.first().copied(),
            };
            let Some(partition) = next else {
                // Clear recovery bookkeeping incrementally. This is a local
                // generation transition, not proof every broker saw the epoch.
                if self.unresolved_partitions.pop_first().is_some() {
                    self.identity_install = Some(install);
                    return Ok(progress);
                }
                self.identity = install.identity;
                self.recovery = RecoveryState::Active;
                progress.complete = true;
                return Ok(progress);
            };
            install.current = Some(PartitionInstall {
                partition,
                index: 0,
                next: Sequence::ZERO,
            });
        }
        let current = install.current.as_mut().expect("selected partition");
        let key = current.partition;
        if let Some(partition) = self.partitions.get_mut(&key) {
            if current.index < partition.entries.len() {
                let entry = &mut partition.entries[current.index];
                // begin_identity_refresh rejected all possible old writes, and
                // RefreshingIdentity prevents new ones throughout installation.
                assert!(!entry.transmitted && entry.current.is_none() && !entry.prior_ambiguity);
                if let Some(pending) = entry.pending {
                    assert_eq!(pending.outcome.kind, DeliveryKind::NotWritten);
                    let entry = partition
                        .entries
                        .remove(current.index)
                        .expect("indexed entry");
                    assert!(self.live_batches.remove(&entry.assignment.batch));
                    if partition.entries.is_empty() {
                        self.nonempty.remove(&key);
                    }
                    progress.change = Some(IdentityChange::Terminal(entry.terminal(pending)));
                    self.refresh_counts(key);
                } else {
                    let before = entry.assignment;
                    entry.assignment.identity = install.identity;
                    entry.assignment.base_sequence = current.next;
                    current.next = current.next.advance(entry.assignment.record_count);
                    current.index += 1;
                    progress.change = Some(IdentityChange::Refinalized(Refinalization {
                        before,
                        after: entry.assignment,
                    }));
                }
                self.identity_install = Some(install);
                return Ok(progress);
            }
            partition.next = current.next;
            partition.sequence_identity = install.identity;
        }
        install.cursor = Some(key);
        install.current = None;
        self.identity_install = Some(install);
        Ok(progress)
    }
}

#[cfg(test)]
mod tests;
