//! Deterministic entity tags for simulated objects.
//!
//! Real S3 etags are content hashes; the simulation needs only determinism
//! and content sensitivity, so a dependency-free FNV-1a 64 suffices.

pub(crate) fn compute(body: &[u8]) -> String {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for byte in body {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    format!("\"{hash:016x}\"")
}
