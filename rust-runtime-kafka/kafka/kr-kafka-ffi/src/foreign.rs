//! Audited foreign immutable storage. Its pin is an ABI caller obligation, not
//! a callback invoked by Rust. The release-event guard is attached by InputLeases.
use kr_shared_bytes::ByteOwner;

pub(crate) struct ForeignMemory {
    pointer: *const u8,
    length: usize,
}
impl ForeignMemory {
    /// # Safety
    /// The complete span is readable and immutable across threads until the
    /// successful registration's InputReleased event, or until destroy returns.
    /// Registration failure ends the obligation immediately.
    pub(crate) unsafe fn new(pointer: *const u8, length: usize) -> Self {
        Self { pointer, length }
    }
}
// SAFETY: construction requires the caller to pin immutable storage across all
// producer threads until the last retained view publishes its release event.
unsafe impl Send for ForeignMemory {}
// SAFETY: no mutation is exposed; the construction contract excludes external
// writes until every shared reader has retired and release is observable.
unsafe impl Sync for ForeignMemory {}
impl ByteOwner for ForeignMemory {
    fn bytes(&self) -> &[u8] {
        // SAFETY: only the audited constructor creates this owner; its caller
        // pins the complete initialized immutable span through the release event.
        unsafe { std::slice::from_raw_parts(self.pointer, self.length) }
    }
}
