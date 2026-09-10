use super::CreditError;
use crate::{
    config::{DescriptorAdmissionPolicy, ProducerConfig},
    types::TopicId,
};
use std::collections::BTreeMap;

/// Admission identity is retained until descriptor settlement, independently of
/// lane changes and broker leadership. Deferred routes share one conservative class.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub enum DescriptorClass {
    #[default]
    Unclassified,
    Partition {
        topic: TopicId,
        partition: i32,
    },
}

/// Exact state before the first descriptor denied by the pressure predicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionPressure {
    /// Partition -1 identifies the conservative unclassified class. Topic
    /// identity comes from the rejected submission, keeping nested errors small.
    pub partition: i32,
    pub capacity: u32,
    pub total_held: u32,
    pub class_held: u32,
}
impl PartitionPressure {
    #[must_use]
    pub fn shared_limit(self) -> u32 {
        self.capacity - self.capacity.div_ceil(4)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Account {
    class: DescriptorClass,
    held: usize,
}

#[derive(Debug)]
pub(super) struct PartitionCredits {
    capacity: usize,
    accounts: Vec<Account>,
    free: Vec<u32>,
    // Only live classes have index nodes; at most capacity entries. Tree-node
    // allocation remains in MemoryBudgetReport's explicit OrderedIndexes gap.
    by_class: BTreeMap<DescriptorClass, u32>,
}

pub(crate) fn configured_storage_bytes(config: &ProducerConfig) -> Option<usize> {
    if config.descriptor_admission_policy == DescriptorAdmissionPolicy::Shared {
        return Some(0);
    }
    (config.record_descriptors as usize).checked_mul(size_of::<Account>() + size_of::<u32>())
}

impl PartitionCredits {
    pub(super) fn is_empty(&self) -> bool {
        self.by_class.is_empty() && self.free.len() == self.capacity
    }
    pub(super) fn new(capacity: usize) -> Result<Self, CreditError> {
        if !(4..=u32::MAX as usize).contains(&capacity) {
            return Err(CreditError::InvalidAmount);
        }
        let mut accounts = Vec::new();
        let mut free = Vec::new();
        accounts
            .try_reserve_exact(capacity)
            .map_err(|_| CreditError::AllocationFailed)?;
        free.try_reserve_exact(capacity)
            .map_err(|_| CreditError::AllocationFailed)?;
        if accounts.capacity() != capacity || free.capacity() != capacity {
            return Err(CreditError::AllocationFailed);
        }
        accounts.resize(capacity, Account::default());
        free.extend((0..capacity as u32).rev());
        Ok(Self {
            capacity,
            accounts,
            free,
            by_class: BTreeMap::new(),
        })
    }
    pub(super) fn check(
        &self,
        class: DescriptorClass,
        total: usize,
        amount: usize,
    ) -> Result<(), CreditError> {
        if amount == 0 {
            return Ok(());
        }
        let held = self
            .by_class
            .get(&class)
            .map_or(0, |slot| self.accounts[*slot as usize].held);
        // floor(3*C/4), without overflowing usize.
        let shared_limit = self.capacity - self.capacity.div_ceil(4);
        let unrestricted = shared_limit.saturating_sub(total);
        // At candidate index k the rule is held+k+1 <= capacity-total-k.
        let pressure_allowed = (self.capacity - total).saturating_sub(held).div_ceil(2);
        let allowed = unrestricted.max(pressure_allowed);
        if amount > allowed {
            let partition = match class {
                DescriptorClass::Unclassified => -1,
                DescriptorClass::Partition { partition, .. } => partition,
            };
            return Err(CreditError::PartitionPressure(PartitionPressure {
                partition,
                capacity: self.capacity as u32,
                total_held: (total + allowed) as u32,
                class_held: (held + allowed) as u32,
            }));
        }
        Ok(())
    }
    pub(super) fn acquire(&mut self, class: DescriptorClass, amount: usize) -> u32 {
        let slot = *self.by_class.entry(class).or_insert_with(|| {
            let slot = self
                .free
                .pop()
                .expect("a new class requires a free descriptor");
            self.accounts[slot as usize].class = class;
            slot
        });
        self.accounts[slot as usize].held += amount;
        slot + 1
    }
    pub(super) fn release(&mut self, one_based: u32, amount: usize) {
        let slot = one_based - 1;
        let account = &mut self.accounts[slot as usize];
        account.held = account
            .held
            .checked_sub(amount)
            .expect("owned descriptor charge");
        if account.held == 0 {
            self.by_class.remove(&account.class);
            self.free.push(slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credit::{Claim, HeldCredits, Resource, SharedCredits};

    fn class(p: u32) -> DescriptorClass {
        DescriptorClass::Partition {
            topic: TopicId([1; 16]),
            partition: p as i32,
        }
    }
    fn reserve(
        credits: &SharedCredits,
        p: u32,
        amount: usize,
        lane: u8,
    ) -> Result<HeldCredits, CreditError> {
        credits.reserve_class(
            &[Claim {
                resource: Resource::Descriptors,
                amount,
                lane,
            }],
            class(p),
        )
    }
    fn reconcile(credits: &SharedCredits) {
        let ledger = credits.0.lock().unwrap();
        let p = ledger.partition.as_ref().unwrap();
        assert_eq!(
            p.accounts.iter().map(|a| a.held).sum::<usize>(),
            ledger.pools[Resource::Descriptors as usize].held
        );
        assert_eq!(p.by_class.len() + p.free.len(), p.capacity);
        for (&class, &slot) in &p.by_class {
            assert_eq!(p.accounts[slot as usize].class, class);
            assert!(p.accounts[slot as usize].held > 0);
        }
    }
    #[test]
    fn batched_predicate_matches_a_record_at_a_time_reference_at_every_small_boundary() {
        for capacity in 4..=32 {
            for total in 0..=capacity {
                for held in 0..=total {
                    let mut ledger = PartitionCredits::new(capacity).unwrap();
                    if held != 0 {
                        ledger.acquire(class(0), held);
                    }
                    if total != held {
                        ledger.acquire(class(1), total - held);
                    }
                    for amount in 0..=capacity - total {
                        let expected = (0..amount).all(|i| {
                            total + i < 3 * capacity / 4 || held + i < capacity - total - i
                        });
                        assert_eq!(
                            ledger.check(class(0), total, amount).is_ok(),
                            expected,
                            "capacity={capacity} total={total} held={held} amount={amount}"
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn scoped_ownership_survives_splits_shrink_and_lane_transfer_without_growth() {
        let credits = SharedCredits::with_descriptor_policy(
            [16; Resource::COUNT],
            2,
            DescriptorAdmissionPolicy::PartitionPressure,
        )
        .unwrap();
        let mut hot = reserve(&credits, 0, 6, 0).unwrap();
        let other = reserve(&credits, 1, 6, 1).unwrap();
        assert!(matches!(
            reserve(&credits, 0, 1, 0),
            Err(CreditError::PartitionPressure(_))
        ));
        let mut cold = credits
            .reserve_class(
                &[
                    Claim {
                        resource: Resource::Descriptors,
                        amount: 1,
                        lane: 0,
                    },
                    Claim {
                        resource: Resource::Descriptors,
                        amount: 1,
                        lane: 0,
                    },
                    Claim {
                        resource: Resource::InputBytes,
                        amount: 3,
                        lane: 0,
                    },
                ],
                class(2),
            )
            .unwrap();
        let mut input = cold.take(Resource::InputBytes);
        HeldCredits::transfer_lane_group(&mut [&mut cold, &mut input], 1).unwrap();
        hot.shrink(Resource::Descriptors, 2).unwrap();
        let detached = cold.take(Resource::Descriptors);
        reconcile(&credits);
        drop((hot, other, detached, input, cold));
        reconcile(&credits);
        assert!(credits.is_empty());
        assert_eq!(size_of::<super::super::GuardCredit>(), 16);
    }
    #[test]
    fn seeded_sparse_4096_destination_campaign_conserves_credits_and_reuses_accounts() {
        for seed in 0..64u64 {
            let credits = SharedCredits::with_descriptor_policy(
                [64; Resource::COUNT],
                1,
                DescriptorAdmissionPolicy::PartitionPressure,
            )
            .unwrap();
            let mut rng = seed + 1;
            let mut owners: Vec<(u32, usize, HeldCredits)> = Vec::new();
            let mut counts = vec![0usize; 4096];
            for step in 0..512 {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                if !owners.is_empty() && rng.is_multiple_of(3) {
                    let index = (rng as usize >> 8) % owners.len();
                    let (p, n, guard) = owners.swap_remove(index);
                    counts[p as usize] -= n;
                    drop(guard);
                } else {
                    let p = if step % 4 != 0 {
                        0
                    } else {
                        ((rng >> 16) % 4096) as u32
                    };
                    let n = 1 + ((rng >> 32) % 4) as usize;
                    let total: usize = counts.iter().sum();
                    let expected = (0..n).all(|i| {
                        total + i < 64
                            && (total + i < 48 || counts[p as usize] + i < 64 - total - i)
                    });
                    let result = reserve(&credits, p, n, 0);
                    assert_eq!(
                        result.is_ok(),
                        expected,
                        "seed={seed} step={step} p={p} n={n}"
                    );
                    if let Ok(guard) = result {
                        counts[p as usize] += n;
                        owners.push((p, n, guard));
                    }
                }
                reconcile(&credits);
            }
            drop(owners);
            reconcile(&credits);
            assert!(credits.is_empty());
        }
    }
    #[test]
    fn other_pool_failure_and_unclassified_admission_do_not_bypass_pressure() {
        let credits = SharedCredits::with_descriptor_policy(
            [8; Resource::COUNT],
            1,
            DescriptorAdmissionPolicy::PartitionPressure,
        )
        .unwrap();
        let held = reserve(&credits, 0, 6, 0).unwrap();
        let before = credits.snapshot();
        assert!(
            credits
                .reserve_class(
                    &[
                        Claim {
                            resource: Resource::Descriptors,
                            amount: 1,
                            lane: 0
                        },
                        Claim {
                            resource: Resource::InputBytes,
                            amount: 9,
                            lane: 0
                        },
                    ],
                    class(1)
                )
                .is_err()
        );
        assert_eq!(before, credits.snapshot());
        let unclassified = credits
            .reserve(&[Claim {
                resource: Resource::Descriptors,
                amount: 1,
                lane: 0,
            }])
            .unwrap();
        assert!(matches!(
            credits.reserve(&[Claim {
                resource: Resource::Descriptors,
                amount: 1,
                lane: 0
            }]),
            Err(CreditError::PartitionPressure(_))
        ));
        drop((held, unclassified));
        reconcile(&credits);
    }
}
