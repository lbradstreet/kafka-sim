//! Immutable, reference-counted byte spans, independent of any runtime or protocol.
#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;
use alloc::{sync::Arc, vec::Vec};
use core::{fmt, ops::Range};

/// An invalid subview, including its bounds and the source view length.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RangeError {
    pub range: Range<usize>,
    pub len: usize,
}

impl fmt::Display for RangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "byte range {:?} is outside view length {}",
            self.range, self.len
        )
    }
}
impl core::error::Error for RangeError {}

/// An immutable byte owner retained across threads without copying its payload.
///
/// The returned bytes and capacity must remain stable for this owner's lifetime.
/// Owners must have small, non-panicking destructors and must never invoke
/// application callbacks: final release can occur under an I/O provider lock.
/// Foreign pointer validity belongs in a separately audited adapter; this crate
/// never dereferences raw pointers or grants mutation of external storage.
pub trait ByteOwner: Send + Sync {
    fn bytes(&self) -> &[u8];
    fn retained_capacity(&self) -> usize {
        self.bytes().len()
    }
}

/// An immutable view retaining the complete backing allocation.
///
/// Clones and subviews share ownership without copying bytes. A small subview
/// still retains [`Self::allocation_len`] bytes, which resource admission must
/// account for. Ownership is thread-safe for provider and compression workers.
#[derive(Clone)]
pub struct SharedBytes {
    owner: Backing,
    range: Range<usize>,
    // Fields drop in order: backing bytes release before their lifetime guard.
    guard: Option<Arc<dyn Send + Sync>>,
}

// Arc<[u8]>::from(Vec<u8>) allocates a replacement payload and copies the Vec.
// Keeping the Vec behind a small Arc instead preserves its payload pointer and
// charges capacity slack that a tiny view still keeps physically allocated.
#[derive(Clone)]
enum Backing {
    Slice(Arc<[u8]>),
    Vector(Arc<Vec<u8>>),
    External {
        owner: Arc<dyn ByteOwner>,
        capacity: usize,
    },
}
impl Backing {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Slice(bytes) => bytes,
            Self::Vector(bytes) => bytes.as_slice(),
            Self::External { owner, .. } => owner.bytes(),
        }
    }
    fn capacity(&self) -> usize {
        match self {
            Self::Slice(bytes) => bytes.len(),
            Self::Vector(bytes) => bytes.capacity(),
            Self::External { capacity, .. } => *capacity,
        }
    }
    fn try_as_mut(&mut self) -> Option<&mut [u8]> {
        match self {
            Self::Slice(bytes) => Arc::get_mut(bytes),
            Self::Vector(bytes) => Arc::get_mut(bytes).map(Vec::as_mut_slice),
            Self::External { .. } => None,
        }
    }
    fn shares_allocation(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Slice(left), Self::Slice(right)) => Arc::ptr_eq(left, right),
            (Self::Vector(left), Self::Vector(right)) => Arc::ptr_eq(left, right),
            (Self::External { owner: left, .. }, Self::External { owner: right, .. }) => {
                Arc::ptr_eq(left, right)
            }
            _ => false,
        }
    }
    fn strong_count(&self) -> usize {
        match self {
            Self::Slice(bytes) => Arc::strong_count(bytes),
            Self::Vector(bytes) => Arc::strong_count(bytes),
            Self::External { owner, .. } => Arc::strong_count(owner),
        }
    }
}

impl fmt::Debug for SharedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedBytes")
            .field("range", &self.range)
            .field("allocation_len", &self.owner.capacity())
            .field("guarded", &self.guard.is_some())
            .finish()
    }
}

impl PartialEq for SharedBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl Eq for SharedBytes {}

impl SharedBytes {
    /// Retains an immutable external owner. Capacity is snapshotted and never
    /// smaller than the readable allocation; subviews preserve that full charge.
    #[must_use]
    pub fn from_owner(owner: Arc<dyn ByteOwner>) -> Self {
        let end = owner.bytes().len();
        let capacity = owner.retained_capacity().max(end);
        Self {
            owner: Backing::External { owner, capacity },
            range: 0..end,
            guard: None,
        }
    }

    #[must_use]
    pub fn new(owner: Arc<[u8]>) -> Self {
        let end = owner.len();
        Self {
            owner: Backing::Slice(owner),
            range: 0..end,
            guard: None,
        }
    }

    /// Creates a subview without copying its bytes.
    ///
    /// # Errors
    /// Returns the requested range and source view length when out of bounds.
    pub fn slice(&self, range: Range<usize>) -> Result<Self, RangeError> {
        if range.start > range.end || range.end > self.len() {
            return Err(RangeError {
                range,
                len: self.len(),
            });
        }
        // Both additions are bounded by the existing allocation's valid range.
        Ok(Self {
            owner: self.owner.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
            guard: self.guard.clone(),
        })
    }

    /// Attaches a resource obligation to this view and every subsequent clone or
    /// slice. Attach before publishing provider-facing spans: earlier clones do
    /// not acquire this guard retroactively. Allocation identity is unchanged.
    ///
    /// Guards must have small, non-panicking destructors and must not reenter an
    /// I/O provider or invoke application callbacks: a provider may release the
    /// last byte view while reconciling an operation under its internal lock.
    ///
    /// # Errors
    /// Returns the original view if it already has a guard. An existing lifetime
    /// obligation can never be silently replaced or detached.
    pub fn attach_guard(mut self, guard: Arc<dyn Send + Sync>) -> Result<Self, Self> {
        if self.guard.is_some() {
            return Err(self);
        }
        self.guard = Some(guard);
        Ok(self)
    }

    /// Retains an additional obligation without replacing an existing guard.
    /// The guard follows this view's subsequent clones and slices; earlier
    /// clones are unaffected. Payload storage and allocation identity do not change.
    ///
    /// The first guard requires no additional allocation. Each composition with
    /// an existing guard allocates one shared pair of guard handles; callers must
    /// bound the number of compositions when bounding metadata memory. Guards
    /// obey the destructor restrictions of [`Self::attach_guard`].
    #[must_use]
    pub fn retain_guard(mut self, guard: Arc<dyn Send + Sync>) -> Self {
        self.guard = Some(match self.guard.take() {
            Some(previous) => Arc::new((previous, guard)),
            None => guard,
        });
        self
    }

    /// Whether this view already carries a lifetime obligation.
    #[must_use]
    pub fn has_guard(&self) -> bool {
        self.guard.is_some()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.owner.bytes()[self.range.clone()]
    }
    /// Mutates this view only when no other strong or weak references exist.
    /// Shared provider or retry-ledger ownership therefore prevents mutation.
    #[must_use]
    pub fn try_as_mut(&mut self) -> Option<&mut [u8]> {
        self.owner
            .try_as_mut()
            .map(|owner| &mut owner[self.range.clone()])
    }
    #[must_use]
    pub const fn len(&self) -> usize {
        self.range.end - self.range.start
    }
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.range.start == self.range.end
    }
    /// Capacity of the entire retained payload allocation, including bytes
    /// outside this view and unused Vec capacity. Uninitialized spare capacity
    /// is charged but is never exposed by `as_slice` or `try_as_mut`.
    #[must_use]
    pub fn allocation_len(&self) -> usize {
        self.owner.capacity()
    }
    /// Alias useful when accounting for retained buffer capacity.
    #[must_use]
    pub fn retained_capacity(&self) -> usize {
        self.allocation_len()
    }
    #[must_use]
    pub fn shares_allocation(&self, other: &Self) -> bool {
        self.owner.shares_allocation(&other.owner)
    }
    /// A diagnostic snapshot; concurrent clones or drops may change it immediately.
    #[must_use]
    pub fn strong_count(&self) -> usize {
        self.owner.strong_count()
    }
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.as_slice().as_ptr()
    }
}

impl AsRef<[u8]> for SharedBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl From<Arc<[u8]>> for SharedBytes {
    fn from(owner: Arc<[u8]>) -> Self {
        Self::new(owner)
    }
}
impl From<Vec<u8>> for SharedBytes {
    /// Retains the original Vec allocation without copying its payload.
    fn from(bytes: Vec<u8>) -> Self {
        let end = bytes.len();
        Self {
            owner: Backing::Vector(Arc::new(bytes)),
            range: 0..end,
            guard: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicUsize, Ordering};
    struct DropCount(Arc<AtomicUsize>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct External {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }
    impl ByteOwner for External {
        fn bytes(&self) -> &[u8] {
            &self.bytes
        }
        fn retained_capacity(&self) -> usize {
            self.bytes.capacity()
        }
    }
    impl Drop for External {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn external_storage_stays_immutable_and_drops_before_its_guard() {
        struct AfterOwner(Arc<AtomicUsize>);
        impl Drop for AfterOwner {
            fn drop(&mut self) {
                assert_eq!(self.0.load(Ordering::SeqCst), 1);
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let mut payload = Vec::with_capacity(128);
        payload.extend_from_slice(b"abcd");
        let pointer = payload.as_ptr();
        let mut bytes = SharedBytes::from_owner(Arc::new(External {
            bytes: payload,
            drops: drops.clone(),
        }));
        assert_eq!(bytes.as_ptr(), pointer);
        assert_eq!(bytes.retained_capacity(), 128);
        assert!(bytes.try_as_mut().is_none());
        assert!(!bytes.has_guard());
        let bytes = bytes
            .attach_guard(Arc::new(AfterOwner(drops.clone())))
            .unwrap();
        let mut tail = bytes.slice(2..4).unwrap();
        assert!(bytes.shares_allocation(&tail));
        drop(bytes);
        assert!(tail.try_as_mut().is_none());
        assert_eq!(tail.as_slice(), b"cd");
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(tail);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn attached_guard_follows_slices_and_cannot_be_replaced() {
        let drops = Arc::new(AtomicUsize::new(0));
        let bytes = SharedBytes::from(alloc::vec![1, 2, 3]);
        let original = bytes.clone();
        let bytes = bytes
            .attach_guard(Arc::new(DropCount(drops.clone())))
            .unwrap();
        assert!(bytes.shares_allocation(&original));
        let provider = bytes.slice(1..2).unwrap();
        let bytes = bytes.attach_guard(Arc::new(())).unwrap_err();
        drop(bytes);
        drop(original);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(provider);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn composed_guards_follow_views_without_replacing_earlier_obligations() {
        let first = Arc::new(AtomicUsize::new(0));
        let second = Arc::new(AtomicUsize::new(0));
        let bytes = SharedBytes::from(alloc::vec![1, 2, 3])
            .retain_guard(Arc::new(DropCount(first.clone())));
        let earlier = bytes.clone();
        let bytes = bytes.retain_guard(Arc::new(DropCount(second.clone())));
        assert!(earlier.shares_allocation(&bytes));
        let view = bytes.slice(1..2).unwrap();
        drop(bytes);
        assert_eq!(first.load(Ordering::SeqCst), 0);
        assert_eq!(second.load(Ordering::SeqCst), 0);
        drop(view);
        assert_eq!(first.load(Ordering::SeqCst), 0);
        assert_eq!(second.load(Ordering::SeqCst), 1);
        assert_eq!(earlier.as_slice(), &[1, 2, 3]);
        drop(earlier);
        assert_eq!(first.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn nested_views_retain_and_identify_the_entire_allocation() {
        let bytes = SharedBytes::from(alloc::vec![1, 2, 3, 4]);
        let view = bytes.slice(1..4).unwrap().slice(1..2).unwrap();
        assert_eq!(view.as_slice(), &[3]);
        assert_eq!(view.allocation_len(), 4);
        assert!(bytes.shares_allocation(&view));
        assert_eq!(bytes.strong_count(), 2);
        assert!(view.slice(0..2).is_err());
        assert!(view.slice(Range { start: 2, end: 1 }).is_err());
        assert!(view.slice(usize::MAX..usize::MAX).is_err());
        assert_eq!(view.as_ptr(), bytes.as_slice()[2..].as_ptr());
    }
    #[test]
    fn ownership_crosses_threads_without_mutable_access() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SharedBytes>();
    }

    #[test]
    fn vector_conversion_preserves_payload_pointer_and_charges_capacity_slack() {
        let mut source = Vec::with_capacity(4096);
        source.extend_from_slice(&[1, 2, 3, 4]);
        let pointer = source.as_ptr();
        let capacity = source.capacity();
        let bytes = SharedBytes::from(source);
        assert_eq!(
            bytes.as_ptr(),
            pointer,
            "conversion must not move/copy the payload"
        );
        assert_eq!(bytes.len(), 4);
        assert_eq!(bytes.allocation_len(), capacity);
        let mut view = bytes.slice(1..3).unwrap();
        assert_eq!(view.retained_capacity(), 4096);
        assert!(view.try_as_mut().is_none());
        drop(bytes);
        view.try_as_mut().unwrap().copy_from_slice(&[9, 8]);
        assert_eq!(view.as_ptr(), pointer.wrapping_add(1));
        assert_eq!(view.as_slice(), &[9, 8]);
        assert!(
            view.slice(0..3).is_err(),
            "capacity slack must never become readable"
        );
    }

    #[test]
    fn slice_and_vector_backings_preserve_identity_and_guard_drop_order() {
        for bytes in [
            SharedBytes::new(Arc::from([1, 2, 3, 4])),
            SharedBytes::from(alloc::vec![1, 2, 3, 4]),
        ] {
            let pointer = bytes.as_ptr();
            let drops = Arc::new(AtomicUsize::new(0));
            let bytes = bytes
                .attach_guard(Arc::new(DropCount(drops.clone())))
                .unwrap();
            let view = bytes.slice(2..3).unwrap();
            assert!(view.shares_allocation(&bytes));
            assert_eq!(view.strong_count(), 2);
            drop(bytes);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert_eq!(view.as_ptr(), pointer.wrapping_add(2));
            drop(view);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn vector_payload_owner_retires_before_the_last_resource_guard() {
        struct OwnerRetired(alloc::sync::Weak<Vec<u8>>, Arc<AtomicUsize>);
        impl Drop for OwnerRetired {
            fn drop(&mut self) {
                assert!(
                    self.0.upgrade().is_none(),
                    "payload owner outlived its credit guard"
                );
                self.1.fetch_add(1, Ordering::SeqCst);
            }
        }
        let bytes = SharedBytes::from(alloc::vec![1, 2, 3]);
        let Backing::Vector(owner) = &bytes.owner else {
            unreachable!()
        };
        let weak = Arc::downgrade(owner);
        let drops = Arc::new(AtomicUsize::new(0));
        let bytes = bytes
            .attach_guard(Arc::new(OwnerRetired(weak, drops.clone())))
            .unwrap();
        let view = bytes.slice(1..2).unwrap();
        drop(bytes);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(view);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
