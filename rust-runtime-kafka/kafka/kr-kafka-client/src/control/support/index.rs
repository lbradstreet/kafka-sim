//! Owned, precharged scratch indexes for one synchronous codec quantum.
use super::{ControlError, OwnedBudget, Result};

struct Entry<K> {
    key: K,
    original: usize,
    seen: bool,
}
pub struct Index<K> {
    entries: Vec<Entry<K>>,
}
impl<K: Ord> Index<K> {
    pub fn new(
        keys: impl ExactSizeIterator<Item = K>,
        budget: &mut OwnedBudget,
        maximum: usize,
        label: &'static str,
        duplicate: ControlError,
    ) -> Result<Self> {
        let mut entries = budget.vec(keys.len(), maximum, label)?;
        entries.extend(keys.enumerate().map(|(original, key)| Entry {
            key,
            original,
            seen: false,
        }));
        entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        if entries.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(duplicate);
        }
        Ok(Self { entries })
    }
    pub fn get(&self, key: &K) -> Option<(usize, usize)> {
        let slot = self
            .entries
            .binary_search_by(|entry| entry.key.cmp(key))
            .ok()?;
        Some((slot, self.entries[slot].original))
    }
    pub fn lower_bound(&self, key: &K) -> Option<&K> {
        let index = self.entries.partition_point(|entry| entry.key < *key);
        self.entries.get(index).map(|entry| &entry.key)
    }
    pub fn mark(&mut self, slot: usize) -> bool {
        !std::mem::replace(&mut self.entries[slot].seen, true)
    }
    /// `sorted` has the same unique key order as this index. Restore the exact
    /// caller order by in-place permutation cycles, with at most n-1 swaps.
    pub fn restore_order<T>(mut self, sorted: &mut [T]) {
        assert_eq!(sorted.len(), self.entries.len());
        for source in 0..sorted.len() {
            while self.entries[source].original != source {
                let target = self.entries[source].original;
                sorted.swap(source, target);
                self.entries.swap(source, target);
            }
        }
    }
}
pub fn unique<T: Ord>(values: &mut [T], error: ControlError) -> Result<()> {
    values.sort_unstable();
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        Err(error)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
