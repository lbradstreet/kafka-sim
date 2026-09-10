//! Passive saturating transport counters. Recording allocates nothing, invokes
//! no callbacks, reads no clock and consumes no random draws.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransportTelemetrySnapshot {
    pub tls_instrumented: bool,
    pub tls_ciphertext_bytes_confirmed: u64,
    /// Explicit adapter copies, excluding crypto-library internal work.
    pub tls_ciphertext_copy_bytes: u64,
    pub tls_plaintext_copy_bytes: u64,
    pub overflowed: bool,
}
#[derive(Debug, Default)]
pub struct TransportTelemetry {
    instrumented: AtomicBool,
    ciphertext: AtomicU64,
    ciphertext_copy: AtomicU64,
    plaintext_copy: AtomicU64,
    overflowed: AtomicBool,
}
impl TransportTelemetry {
    fn add(&self, counter: &AtomicU64, value: u64) {
        let old = counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(old.saturating_add(value))
            })
            .unwrap();
        if old.checked_add(value).is_none() {
            self.overflowed.store(true, Ordering::Relaxed);
        }
    }
    pub fn snapshot(&self) -> TransportTelemetrySnapshot {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        TransportTelemetrySnapshot {
            tls_instrumented: self.instrumented.load(Ordering::Relaxed),
            tls_ciphertext_bytes_confirmed: read(&self.ciphertext),
            tls_ciphertext_copy_bytes: read(&self.ciphertext_copy),
            tls_plaintext_copy_bytes: read(&self.plaintext_copy),
            overflowed: self.overflowed.load(Ordering::Relaxed),
        }
    }
    /// Marks instrumentation before any TLS operation can be admitted.
    pub fn instrument_tls(&self) {
        self.instrumented.store(true, Ordering::Relaxed);
    }
    /// Records only transport-confirmed ciphertext prefixes.
    pub fn tls_write(&self, confirmed: usize) {
        self.add(&self.ciphertext, confirmed as u64);
    }
    pub fn tls_copy(&self, ciphertext: usize, plaintext: usize) {
        self.add(&self.ciphertext_copy, ciphertext as u64);
        self.add(&self.plaintext_copy, plaintext as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counters_saturate_independently_and_latch_overflow() {
        let telemetry = TransportTelemetry::default();
        telemetry.instrument_tls();
        telemetry.add(&telemetry.ciphertext, u64::MAX);
        telemetry.tls_write(1);
        telemetry.tls_copy(7, 11);
        let snapshot = telemetry.snapshot();
        assert!(snapshot.tls_instrumented);
        assert_eq!(snapshot.tls_ciphertext_bytes_confirmed, u64::MAX);
        assert_eq!(snapshot.tls_ciphertext_copy_bytes, 7);
        assert_eq!(snapshot.tls_plaintext_copy_bytes, 11);
        assert!(snapshot.overflowed);
    }
}
