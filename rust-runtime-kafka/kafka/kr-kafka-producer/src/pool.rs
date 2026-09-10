//! Bounded generation-checked object slots. Retired generations never wrap.
use std::{fmt, marker::PhantomData};

/// A typed identity that cannot alias a replacement object at the same index.
#[repr(C)]
pub struct Slot<T> {
    index: u32,
    generation: u32,
    marker: PhantomData<fn() -> T>,
}
impl<T> Copy for Slot<T> {}
impl<T> Clone for Slot<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> PartialEq for Slot<T> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.generation == other.generation
    }
}
impl<T> Eq for Slot<T> {}
impl<T> PartialOrd for Slot<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for Slot<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.index, self.generation).cmp(&(other.index, other.generation))
    }
}
impl<T> std::hash::Hash for Slot<T> {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        self.index.hash(h);
        self.generation.hash(h);
    }
}
impl<T> fmt::Debug for Slot<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot")
            .field("index", &self.index)
            .field("generation", &self.generation)
            .finish()
    }
}
impl<T> Slot<T> {
    #[must_use]
    pub const fn index(self) -> u32 {
        self.index
    }
    #[must_use]
    pub const fn generation(self) -> u32 {
        self.generation
    }
    #[must_use]
    pub const fn packed(self) -> u64 {
        (self.generation as u64) << 32 | self.index as u64
    }
    /// Constructs an untrusted key; lookup still validates its generation.
    #[must_use]
    pub const fn from_packed(value: u64) -> Self {
        Self {
            index: value as u32,
            generation: (value >> 32) as u32,
            marker: PhantomData,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PoolError {
    ResourceExhausted { limit: usize },
    AllocationFailed,
    StaleKey { index: u32, generation: u32 },
}
impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceExhausted { limit } => write!(f, "object pool exhausted ({limit} slots)"),
            Self::AllocationFailed => f.write_str("object pool allocation failed"),
            Self::StaleKey { index, generation } => write!(f, "stale slot {index}:{generation}"),
        }
    }
}
impl std::error::Error for PoolError {}

#[derive(Debug)]
pub struct InsertFailure<T> {
    pub error: PoolError,
    pub value: T,
}
struct Entry<T> {
    generation: u32,
    value: Option<T>,
}
/// A fixed-limit arena. Removal is explicit and returns the stored object.
pub struct Pool<T> {
    entries: Vec<Entry<T>>,
    free: Vec<u32>,
    limit: usize,
    len: usize,
}
impl<T> Pool<T> {
    /// Exact retained entry/free-stack backing. Nested owners and transient
    /// constructor reservation capacity are not included.
    pub(crate) fn configured_storage_bytes(limit: usize) -> Option<usize> {
        limit.checked_mul(size_of::<Entry<T>>().checked_add(size_of::<u32>())?)
    }

    #[cfg(test)]
    pub(crate) fn storage_capacity_bytes(&self) -> usize {
        self.entries.capacity() * size_of::<Entry<T>>() + self.free.capacity() * size_of::<u32>()
    }

    /// # Errors
    /// Rejects capacities outside the key's index range or allocation failure.
    /// Reservations with capacity above the requested limit are also rejected.
    pub fn new(limit: usize) -> Result<Self, PoolError> {
        if limit > u32::MAX as usize {
            return Err(PoolError::ResourceExhausted { limit });
        }
        let entries = crate::fixed::try_vec(limit).map_err(|_| PoolError::AllocationFailed)?;
        let free = crate::fixed::try_vec(limit).map_err(|_| PoolError::AllocationFailed)?;
        Ok(Self {
            entries,
            free,
            limit,
            len: 0,
        })
    }
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
    /// # Errors
    /// Returns the input unchanged when no fresh generation is available.
    pub fn insert(&mut self, value: T) -> Result<Slot<T>, InsertFailure<T>> {
        let (index, generation) = if let Some(&index) = self.free.last() {
            // A slot enters free only when its generation can advance.
            let Some(generation) = self.entries[index as usize].generation.checked_add(1) else {
                return Err(InsertFailure {
                    error: PoolError::ResourceExhausted { limit: self.limit },
                    value,
                });
            };
            self.free.pop();
            (index, generation)
        } else {
            if self.entries.len() == self.limit {
                return Err(InsertFailure {
                    error: PoolError::ResourceExhausted { limit: self.limit },
                    value,
                });
            }
            let index = self.entries.len() as u32;
            self.entries.push(Entry {
                generation: 0,
                value: None,
            });
            (index, 1)
        };
        self.entries[index as usize] = Entry {
            generation,
            value: Some(value),
        };
        self.len += 1;
        Ok(Slot {
            index,
            generation,
            marker: PhantomData,
        })
    }
    #[must_use]
    pub fn get(&self, key: Slot<T>) -> Option<&T> {
        let e = self.entries.get(key.index as usize)?;
        (e.generation == key.generation)
            .then_some(e.value.as_ref())
            .flatten()
    }
    pub fn get_mut(&mut self, key: Slot<T>) -> Option<&mut T> {
        let e = self.entries.get_mut(key.index as usize)?;
        (e.generation == key.generation)
            .then_some(e.value.as_mut())
            .flatten()
    }
    /// # Errors
    /// Rejects stale keys without changing live objects or the free list.
    pub fn remove(&mut self, key: Slot<T>) -> Result<T, PoolError> {
        let error = PoolError::StaleKey {
            index: key.index,
            generation: key.generation,
        };
        let entry = self.entries.get_mut(key.index as usize).ok_or(error)?;
        if entry.generation != key.generation {
            return Err(error);
        }
        let value = entry.value.take().ok_or(error)?;
        self.len -= 1;
        if entry.generation < u32::MAX {
            self.free.push(key.index);
        }
        Ok(value)
    }
    /// Number of materialized slots, including holes. Resumable maintenance
    /// charges one visited slot rather than searching unbounded runs of holes.
    #[must_use]
    pub fn allocated_slots(&self) -> usize {
        self.entries.len()
    }
    #[must_use]
    pub fn key_at(&self, index: usize) -> Option<Slot<T>> {
        let entry = self.entries.get(index)?;
        entry.value.as_ref().map(|_| Slot {
            index: index as u32,
            generation: entry.generation,
            marker: PhantomData,
        })
    }
    pub fn iter(&self) -> impl Iterator<Item = (Slot<T>, &T)> {
        self.entries.iter().enumerate().filter_map(|(i, e)| {
            e.value.as_ref().map(|v| {
                (
                    Slot {
                        index: i as u32,
                        generation: e.generation,
                        marker: PhantomData,
                    },
                    v,
                )
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_reservation_preserves_lazy_frontier_and_lifo_reuse() {
        let mut pool = Pool::new(17).unwrap();
        let storage = pool.storage_capacity_bytes();
        assert_eq!(pool.allocated_slots(), 0);
        assert_eq!(pool.iter().count(), 0);
        assert_eq!(pool.entries.len(), 0);
        assert_eq!(pool.free.len(), 0);
        let first = pool.insert(1).unwrap();
        let second = pool.insert(2).unwrap();
        pool.remove(first).unwrap();
        pool.remove(second).unwrap();
        assert_eq!(pool.allocated_slots(), 2);
        let replacement = pool.insert(3).unwrap();
        assert_eq!(replacement.index(), second.index());
        assert_eq!(replacement.generation(), second.generation() + 1);
        assert_eq!(pool.allocated_slots(), 2);
        assert_eq!(pool.key_at(16), None);
        assert_eq!(pool.storage_capacity_bytes(), storage);

        let mut empty = Pool::new(0).unwrap();
        assert_eq!(empty.storage_capacity_bytes(), 0);
        assert_eq!(empty.insert(42).unwrap_err().value, 42);
        assert_eq!(empty.allocated_slots(), 0);
    }
    #[test]
    fn pool_storage_does_not_grow_when_full_slots_are_reused() {
        assert!(Pool::<u64>::configured_storage_bytes(usize::MAX).is_none());
        let mut pool = Pool::<u64>::new(7).unwrap();
        let storage = pool.storage_capacity_bytes();
        assert_eq!(storage, Pool::<u64>::configured_storage_bytes(7).unwrap());
        for generation in 0..100 {
            let keys: Vec<_> = (0..7)
                .map(|i| pool.insert(generation * 7 + i).unwrap())
                .collect();
            assert!(pool.insert(700).is_err());
            for key in keys {
                pool.remove(key).unwrap();
            }
            assert_eq!(pool.storage_capacity_bytes(), storage);
        }
    }
    #[test]
    fn stale_slots_never_refer_to_reused_objects() {
        let mut p = Pool::new(1).unwrap();
        let first = p.insert(7).unwrap();
        assert_eq!(p.remove(first), Ok(7));
        let second = p.insert(9).unwrap();
        assert_eq!(first.index(), second.index());
        assert_ne!(first, second);
        assert!(p.get(first).is_none());
        assert!(p.remove(first).is_err());
        assert_eq!(p.get(second), Some(&9));
        assert_eq!(p.insert(42).unwrap_err().value, 42);
        assert_eq!(p.len(), 1);
    }
    #[test]
    fn exhausted_generation_is_retired_without_wrapping() {
        let mut p = Pool::new(1).unwrap();
        let key = p.insert(7).unwrap();
        p.entries[0].generation = u32::MAX;
        let last = Slot::from_packed((u32::MAX as u64) << 32);
        assert_eq!(p.remove(last), Ok(7));
        assert!(p.insert(8).is_err());
        assert!(p.get(key).is_none());
        assert!(p.is_empty());
    }
    #[test]
    fn deterministic_reuse_campaign_preserves_live_key_values() {
        for seed in 1u64..64 {
            let mut state = seed;
            let mut p = Pool::new(17).unwrap();
            let mut live = Vec::new();
            let mut stale = Vec::new();
            for step in 0..256 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                if state & 1 == 0 && live.len() < 17 {
                    let key = p.insert(step).unwrap();
                    live.push((key, step));
                } else if !live.is_empty() {
                    let i = state as usize % live.len();
                    let (key, value) = live.swap_remove(i);
                    assert_eq!(p.remove(key), Ok(value));
                    stale.push(key);
                }
                assert_eq!(p.len(), live.len(), "seed={seed} step={step}");
                for &(key, value) in &live {
                    assert_eq!(p.get(key), Some(&value));
                }
                for &key in &stale {
                    assert!(p.get(key).is_none(), "seed={seed} step={step}");
                }
            }
        }
    }
}
