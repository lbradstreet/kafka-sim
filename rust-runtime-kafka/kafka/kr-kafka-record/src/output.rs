use crate::{BATCH_HEADER_BYTES, Error, Result, SharedBytes};
use alloc::{boxed::Box, collections::BTreeMap, rc::Rc, sync::Arc, vec::Vec};
use core::{
    cell::RefCell,
    mem::size_of,
    sync::atomic::{AtomicUsize, Ordering},
    task::Waker,
};
use futures_util::task::AtomicWaker;

const NONE: usize = usize::MAX;

/// An owner-local hard reservation pool. Status is observational: charges can
/// conservatively lag the last provider reference until `reclaim_step` runs.
/// Every public chunk carries a release token; retained provider views prevent
/// reuse without requiring reference-count scans or a callback into the owner.
#[derive(Clone)]
pub struct OutputPool {
    inner: Rc<RefCell<Pool>>,
}
struct Pool {
    maximum: usize,
    reserved: usize,
    allocated: usize,
    batches: usize,
    slots: Vec<Option<Reservation>>,
    free: Vec<usize>,
    releases: Arc<ReleaseQueue>,
    pending: usize,
    retiring: Option<usize>,
    cache: Cache,
    requested_headroom: usize,
    clear_cache: bool,
    metadata_bytes: usize,
}
struct Reservation {
    envelope: usize,
    allocated: usize,
    chunks: Vec<SharedBytes>,
}
struct ReleaseQueue {
    head: AtomicUsize,
    next: Box<[AtomicUsize]>,
    waker: AtomicWaker,
}
struct ReleaseToken {
    queue: Arc<ReleaseQueue>,
    index: usize,
}
impl Drop for ReleaseToken {
    fn drop(&mut self) {
        // Each slot publishes once and is reused only after owner-side reaping.
        // A fixed intrusive MPSC stack cannot saturate or lose a release.
        let mut head = self.queue.head.load(Ordering::Relaxed);
        loop {
            self.queue.next[self.index].store(head, Ordering::Relaxed);
            match self.queue.head.compare_exchange_weak(
                head,
                self.index,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => head = next,
            }
        }
        self.queue.waker.wake();
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.releases.waker.take();
    }
}
struct CacheEntry {
    bytes: SharedBytes,
    next: Option<usize>,
}
struct Cache {
    heads: BTreeMap<usize, usize>,
    slots: Vec<Option<CacheEntry>>,
    free: Vec<usize>,
    bytes: usize,
}
impl Cache {
    fn take(&mut self, length: usize) -> Option<SharedBytes> {
        let index = *self.heads.get(&length)?;
        let entry = self.slots[index].take().expect("indexed cache entry");
        if let Some(next) = entry.next {
            self.heads.insert(length, next);
        } else {
            self.heads.remove(&length);
        }
        self.free.push(index);
        self.bytes -= length;
        Some(entry.bytes)
    }
    fn insert(&mut self, bytes: SharedBytes) -> bool {
        // Very small final payload fragments are not useful cache entries.
        if bytes.len() < BATCH_HEADER_BYTES || self.free.is_empty() {
            return false;
        }
        let index = self.free.pop().expect("free cache slot");
        let length = bytes.allocation_len();
        let next = self.heads.insert(length, index);
        self.slots[index] = Some(CacheEntry { bytes, next });
        self.bytes += length;
        true
    }
    fn evict(&mut self) -> usize {
        let Some((&length, _)) = self.heads.first_key_value() else {
            return 0;
        };
        drop(self.take(length));
        length
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OutputPoolStatus {
    pub capacity_bytes: usize,
    pub reserved_bytes: usize,
    /// Charged backing capacity, including cached allocations and releases that
    /// await budgeted reaping. Never smaller than retained payload capacity.
    pub allocated_bytes: usize,
    pub cached_bytes: usize,
    pub batches: usize,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OutputReclaimProgress {
    pub work_items: usize,
    pub released_batches: usize,
    pub released_reserved_bytes: usize,
    pub freed_allocated_bytes: usize,
    pub remaining: bool,
}
fn capacity_bytes<T>(capacity: usize) -> Result<usize> {
    capacity
        .checked_mul(size_of::<T>())
        .ok_or(Error::LengthOverflow)
}
fn slots<T>(count: usize) -> Result<Vec<Option<T>>> {
    let mut slots = Vec::new();
    slots
        .try_reserve_exact(count)
        .map_err(|_| Error::AllocationFailed)?;
    slots.resize_with(count, || None);
    Ok(slots)
}
fn free_slots(count: usize) -> Result<Vec<usize>> {
    let mut free = Vec::new();
    free.try_reserve_exact(count)
        .map_err(|_| Error::AllocationFailed)?;
    free.extend((0..count).rev());
    Ok(free)
}
impl OutputPool {
    /// Convenience bound derived from the minimum allocation size. Producers
    /// should use `with_limits` to bound metadata by their configured batch cap.
    pub fn new(capacity_bytes: usize) -> Result<Self> {
        Self::with_limits(
            capacity_bytes,
            capacity_bytes / BATCH_HEADER_BYTES,
            capacity_bytes / BATCH_HEADER_BYTES,
        )
    }
    pub fn with_limits(
        capacity: usize,
        max_batches: usize,
        max_cached_chunks: usize,
    ) -> Result<Self> {
        if capacity < BATCH_HEADER_BYTES
            || max_batches == 0
            || max_batches > capacity / BATCH_HEADER_BYTES
            || max_cached_chunks > capacity / BATCH_HEADER_BYTES
        {
            return Err(Error::InvalidConfig);
        }
        let slots = slots(max_batches)?;
        let free = free_slots(max_batches)?;
        let cache = Cache {
            heads: BTreeMap::new(),
            slots: self::slots(max_cached_chunks)?,
            free: free_slots(max_cached_chunks)?,
            bytes: 0,
        };
        let mut next = Vec::new();
        next.try_reserve_exact(max_batches)
            .map_err(|_| Error::AllocationFailed)?;
        next.extend((0..max_batches).map(|_| AtomicUsize::new(NONE)));
        let releases = Arc::new(ReleaseQueue {
            head: AtomicUsize::new(NONE),
            next: next.into_boxed_slice(),
            waker: AtomicWaker::new(),
        });
        let metadata_bytes = [
            capacity_bytes::<Option<Reservation>>(slots.capacity())?,
            capacity_bytes::<usize>(free.capacity())?,
            capacity_bytes::<Option<CacheEntry>>(cache.slots.capacity())?,
            capacity_bytes::<usize>(cache.free.capacity())?,
            capacity_bytes::<AtomicUsize>(releases.next.len())?,
        ]
        .into_iter()
        .try_fold(0usize, |n, v| n.checked_add(v).ok_or(Error::LengthOverflow))?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Pool {
                maximum: capacity,
                reserved: 0,
                allocated: 0,
                batches: 0,
                slots,
                free,
                releases,
                pending: NONE,
                retiring: None,
                cache,
                requested_headroom: 0,
                clear_cache: false,
                metadata_bytes,
            })),
        })
    }
    pub fn status(&self) -> OutputPoolStatus {
        let pool = self.inner.borrow();
        OutputPoolStatus {
            capacity_bytes: pool.maximum,
            reserved_bytes: pool.reserved,
            allocated_bytes: pool.allocated,
            cached_bytes: pool.cache.bytes,
            batches: pool.batches,
        }
    }
    /// Exact observed element backing bytes for the fixed slot arrays and live
    /// reservation vectors. Excludes Rc/Arc control blocks and BTreeMap's private
    /// node layout (bounded separately by `cache_index_entries`). No payload copy.
    pub fn metadata_capacity_bytes(&self) -> Result<usize> {
        Ok(self.inner.borrow().metadata_bytes)
    }
    pub fn cache_index_entries(&self) -> usize {
        self.inner.borrow().cache.heads.len()
    }
    /// Register on each owner poll, then check `has_reclaim_work`. The waker must
    /// only schedule, never synchronously reenter/poll, and must not panic: its
    /// invocation can occur on a provider thread or inside an FFI release callback.
    pub fn register_reclaim_waker(&self, waker: &Waker) {
        self.inner.borrow().releases.waker.register(waker);
    }
    /// Permanently disables caching and requests bounded eviction of existing
    /// cache owners. Use during cooperative shutdown before dropping the pool.
    pub fn request_cache_clear(&self) {
        self.inner.borrow_mut().clear_cache = true;
    }
    pub fn has_reclaim_work(&self) -> bool {
        self.inner.borrow().has_work()
    }
    /// One item releases at most one chunk, or evicts one cached allocation.
    /// Last-reference notification itself performs no allocation or batch scan.
    pub fn reclaim_step(&self, maximum: usize) -> OutputReclaimProgress {
        let mut pool = self.inner.borrow_mut();
        let mut progress = OutputReclaimProgress::default();
        while progress.work_items < maximum {
            if pool.retiring.is_none() {
                if pool.pending == NONE {
                    pool.pending = pool.releases.head.swap(NONE, Ordering::Acquire);
                }
                if pool.pending != NONE {
                    let index = pool.pending;
                    pool.pending = pool.releases.next[index].load(Ordering::Relaxed);
                    pool.retiring = Some(index);
                }
            }
            if let Some(index) = pool.retiring {
                let chunk = pool.slots[index]
                    .as_mut()
                    .expect("retiring reservation")
                    .chunks
                    .pop();
                if let Some(chunk) = chunk {
                    let length = chunk.allocation_len();
                    pool.slots[index].as_mut().expect("reservation").allocated -= length;
                    if pool.clear_cache || !pool.cache.insert(chunk) {
                        pool.allocated -= length;
                        progress.freed_allocated_bytes += length;
                    }
                }
                if pool.slots[index]
                    .as_ref()
                    .expect("reservation")
                    .chunks
                    .is_empty()
                {
                    let batch = pool.slots[index].take().expect("reservation");
                    pool.reserved -= batch.envelope;
                    pool.allocated -= batch.allocated;
                    pool.metadata_bytes -= capacity_bytes::<SharedBytes>(batch.chunks.capacity())
                        .expect("tracked metadata");
                    pool.batches -= 1;
                    pool.free.push(index);
                    pool.retiring = None;
                    progress.released_batches += 1;
                    progress.released_reserved_bytes += batch.envelope;
                    progress.freed_allocated_bytes += batch.allocated;
                }
            } else if pool.cache.bytes != 0
                && (pool.clear_cache || pool.requested_headroom > pool.maximum - pool.allocated)
            {
                let length = pool.cache.evict();
                pool.allocated -= length;
                progress.freed_allocated_bytes += length;
            } else {
                break;
            }
            progress.work_items += 1;
        }
        if maximum != 0 && pool.requested_headroom <= pool.maximum - pool.allocated {
            pool.requested_headroom = 0;
        }
        progress.remaining = pool.has_work();
        progress
    }
    fn reserve(&self, envelope: usize, payload_chunks: usize) -> Result<OutputLease> {
        let mut pool = self.inner.borrow_mut();
        if envelope < BATCH_HEADER_BYTES
            || envelope > pool.maximum - pool.reserved
            || pool.free.is_empty()
        {
            return Err(Error::OutputExhausted);
        }
        // Avoid creating an empty reservation on each cache-pressure retry:
        // with a one-item maintenance budget, repeatedly reaping those empty
        // slots could otherwise starve the eviction that makes header room.
        if BATCH_HEADER_BYTES > pool.maximum - pool.allocated
            && !pool.cache.heads.contains_key(&BATCH_HEADER_BYTES)
        {
            pool.requested_headroom = pool.requested_headroom.max(BATCH_HEADER_BYTES);
            return Err(Error::OutputExhausted);
        }
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(payload_chunks)
            .map_err(|_| Error::AllocationFailed)?;
        let metadata = capacity_bytes::<SharedBytes>(chunks.capacity())?;
        let metadata_bytes = pool
            .metadata_bytes
            .checked_add(metadata)
            .ok_or(Error::LengthOverflow)?;
        let index = pool.free.pop().expect("free reservation");
        pool.slots[index] = Some(Reservation {
            envelope,
            allocated: 0,
            chunks,
        });
        pool.metadata_bytes = metadata_bytes;
        pool.reserved += envelope;
        pool.batches += 1;
        let owner = Arc::new(ReleaseToken {
            queue: pool.releases.clone(),
            index,
        });
        Ok(OutputLease {
            pool: self.clone(),
            index,
            owner,
        })
    }
}
impl Pool {
    fn has_work(&self) -> bool {
        self.pending != NONE
            || self.retiring.is_some()
            || self.releases.head.load(Ordering::Acquire) != NONE
            || (self.cache.bytes != 0
                && (self.clear_cache || self.requested_headroom > self.maximum - self.allocated))
    }
}
pub(crate) struct OutputLease {
    pool: OutputPool,
    index: usize,
    owner: Arc<ReleaseToken>,
}
impl OutputLease {
    fn track(&self, chunks: &[SharedBytes]) {
        let mut pool = self.pool.inner.borrow_mut();
        let batch = pool.slots[self.index].as_mut().expect("live reservation");
        assert!(chunks.len() <= batch.chunks.capacity());
        batch.chunks.extend(chunks.iter().cloned());
    }
    fn chunk(&self, length: usize) -> Result<SharedBytes> {
        let mut pool = self.pool.inner.borrow_mut();
        if let Some(bytes) = pool.cache.take(length) {
            pool.allocated -= length;
            return Ok(bytes);
        }
        if length > pool.maximum - pool.allocated {
            pool.requested_headroom = pool.requested_headroom.max(length);
            return Err(Error::OutputExhausted);
        }
        Ok(SharedBytes::from(Arc::from_iter(core::iter::repeat_n(
            0, length,
        ))))
    }
    pub(crate) fn allocated(&self, bytes: usize) {
        let mut pool = self.pool.inner.borrow_mut();
        let batch = pool.slots[self.index].as_mut().expect("live reservation");
        assert!(bytes <= batch.envelope);
        let old = batch.allocated;
        batch.allocated = bytes;
        pool.allocated = pool.allocated - old + bytes;
    }
    pub(crate) fn shrink(&self, bytes: usize) {
        let mut pool = self.pool.inner.borrow_mut();
        let batch = pool.slots[self.index].as_mut().expect("live reservation");
        let freed = batch.envelope - bytes;
        batch.envelope = bytes;
        pool.reserved -= freed;
    }
}

pub(crate) struct ChunkWriter {
    chunks: Vec<SharedBytes>,
    used: usize,
    allocated: usize,
    limit: usize,
    chunk_bytes: usize,
    pub lease: OutputLease,
}
impl ChunkWriter {
    pub(crate) fn new(pool: &OutputPool, limit: usize, chunk_bytes: usize) -> Result<Self> {
        let payload_chunks = (limit - BATCH_HEADER_BYTES).div_ceil(chunk_bytes);
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(1 + payload_chunks)
            .map_err(|_| Error::AllocationFailed)?;
        let lease = pool.reserve(limit, payload_chunks)?;
        let mut writer = Self {
            chunks,
            used: 0,
            allocated: 0,
            limit,
            chunk_bytes,
            lease,
        };
        writer.extend(&[0; BATCH_HEADER_BYTES])?;
        Ok(writer)
    }
    fn ensure(&mut self) -> Result<()> {
        if self.used == self.allocated {
            let length = if self.chunks.is_empty() {
                BATCH_HEADER_BYTES
            } else {
                self.chunk_bytes.min(self.limit - self.allocated)
            };
            if length == 0 {
                return Err(Error::CompressedTooLarge);
            }
            self.chunks.push(self.lease.chunk(length)?);
            self.allocated += length;
            self.lease.allocated(self.allocated);
        }
        Ok(())
    }
    pub(crate) fn writable(&mut self) -> Result<&mut [u8]> {
        self.ensure()?;
        let (index, offset) = if self.used < BATCH_HEADER_BYTES {
            (0, self.used)
        } else {
            (
                1 + (self.used - BATCH_HEADER_BYTES) / self.chunk_bytes,
                (self.used - BATCH_HEADER_BYTES) % self.chunk_bytes,
            )
        };
        Ok(&mut self.chunks[index].try_as_mut().expect("unpublished output")[offset..])
    }
    pub(crate) fn advance(&mut self, n: usize) {
        assert!(n <= self.allocated - self.used);
        self.used += n;
    }
    fn extend(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            let out = self.writable()?;
            let n = out.len().min(bytes.len());
            out[..n].copy_from_slice(&bytes[..n]);
            self.advance(n);
            bytes = &bytes[n..];
        }
        Ok(())
    }
    pub(crate) fn len(&self) -> usize {
        self.used
    }
    pub(crate) fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
    pub(crate) fn allocated(&self) -> usize {
        self.allocated
    }
    /// Best-effort compaction after the last encoder write, before publication.
    /// The old and new allocations coexist within both the full reservation
    /// and the physical pool. No scratch credit means keep the existing tail.
    pub(crate) fn compact_tail(&mut self, maximum_copy: usize) -> usize {
        if self.chunks.len() <= 1 {
            return 0;
        }
        let old = self.chunks.last().expect("payload tail").allocation_len();
        let used = self.used - (self.allocated - old);
        if used == 0 {
            drop(self.chunks.pop());
            self.allocated -= old;
            self.lease.allocated(self.allocated);
            return 0;
        }
        if used > maximum_copy || old < 1024 || used > old / 2 || used > self.limit - self.allocated
        {
            return 0;
        }
        let Ok(mut compact) = self.lease.chunk(used) else {
            return 0;
        };
        self.lease.allocated(self.allocated + used);
        compact
            .try_as_mut()
            .expect("private scratch")
            .copy_from_slice(&self.chunks.last().expect("payload tail").as_slice()[..used]);
        let tail = self.chunks.last_mut().expect("payload tail");
        drop(core::mem::replace(tail, compact));
        self.allocated = self.allocated - old + used;
        self.lease.allocated(self.allocated);
        used
    }
    pub(crate) fn metadata_capacity_bytes(&self) -> Result<usize> {
        capacity_bytes::<SharedBytes>(self.chunks.capacity())
    }
    pub(crate) fn abort_chunks(&mut self, maximum: usize) -> usize {
        let mut released = 0;
        while released < maximum {
            let Some(chunk) = self.chunks.pop() else {
                break;
            };
            self.allocated -= chunk.allocation_len();
            drop(chunk);
            released += 1;
        }
        if released != 0 {
            self.used = self.used.min(self.allocated);
            self.lease.allocated(self.allocated);
        }
        released
    }
    pub(crate) fn first_mut(&mut self) -> &mut [u8] {
        self.chunks[0].try_as_mut().expect("unpublished output")
    }
    pub(crate) fn crc(&self) -> u32 {
        let mut crc = !0;
        let mut remaining = self.used;
        for (i, chunk) in self.chunks.iter().enumerate() {
            let end = remaining.min(chunk.len());
            crc =
                crate::record::crc_update(crc, &chunk.as_slice()[if i == 0 { 21 } else { 0 }..end]);
            remaining -= end;
        }
        !crc
    }
    pub(crate) fn freeze(mut self) -> (Vec<SharedBytes>, OutputLease) {
        // Only payload allocations get private cache mirrors. Keeping the header
        // separate permits epoch refinalization without removing all mirrors.
        self.lease.track(&self.chunks[1..]);
        let mut remaining = self.used;
        for chunk in &mut self.chunks {
            let n = remaining.min(chunk.len());
            remaining -= n;
            *chunk = chunk
                .slice(0..n)
                .expect("used range")
                .retain_guard(self.lease.owner.clone());
        }
        self.lease.shrink(self.allocated);
        (self.chunks, self.lease)
    }
}
