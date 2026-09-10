//! Cumulative diagnostics shared with host callers. Counters do not drive policy,
//! read clocks, allocate, or consume random draws. Overflow is explicit.
use kr_kafka_client::telemetry::TransportTelemetry;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

pub mod metrics;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TelemetrySnapshot {
    pub actor_polls: u64,
    pub max_host_poll_nanos: u64,
    pub codec_input_bytes: u64,
    pub max_codec_quantum_bytes: u64,
    /// Confirmed plaintext Kafka Produce bytes, including retries and framing.
    pub produce_wire_bytes_confirmed: u64,
    pub produce_staging_copy_bytes: u64,
    pub produce_coalesced_copy_bytes: u64,
    pub tls_instrumented: bool,
    pub tls_ciphertext_bytes_confirmed: u64,
    /// Explicit adapter copies in both directions; excludes crypto-library internals.
    pub tls_ciphertext_copy_bytes: u64,
    pub tls_plaintext_copy_bytes: u64,
    pub overflowed: bool,
}
#[derive(Debug, Default)]
pub struct ProducerTelemetry {
    metrics: OnceLock<metrics::MetricsReader>,
    polls: AtomicU64,
    poll_max: AtomicU64,
    codec: AtomicU64,
    codec_max: AtomicU64,
    wire: AtomicU64,
    staging: AtomicU64,
    coalesced: AtomicU64,
    transport: Arc<TransportTelemetry>,
    overflow: AtomicBool,
}
impl ProducerTelemetry {
    pub(crate) fn attach_metrics(&self, reader: metrics::MetricsReader) {
        self.metrics
            .set(reader)
            .expect("metrics attached once before actor admission");
    }
    pub fn metrics(&self) -> metrics::MetricsReader {
        self.metrics.get().cloned().unwrap_or_default()
    }
    fn add(&self, counter: &AtomicU64, value: u64) {
        let old = counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(old.saturating_add(value))
            })
            .unwrap();
        if old.checked_add(value).is_none() {
            self.overflow.store(true, Ordering::Relaxed);
        }
    }
    pub fn snapshot(&self) -> TelemetrySnapshot {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let transport = self.transport.snapshot();
        TelemetrySnapshot {
            actor_polls: read(&self.polls),
            max_host_poll_nanos: read(&self.poll_max),
            codec_input_bytes: read(&self.codec),
            max_codec_quantum_bytes: read(&self.codec_max),
            produce_wire_bytes_confirmed: read(&self.wire),
            produce_staging_copy_bytes: read(&self.staging),
            produce_coalesced_copy_bytes: read(&self.coalesced),
            tls_instrumented: transport.tls_instrumented,
            tls_ciphertext_bytes_confirmed: transport.tls_ciphertext_bytes_confirmed,
            tls_ciphertext_copy_bytes: transport.tls_ciphertext_copy_bytes,
            tls_plaintext_copy_bytes: transport.tls_plaintext_copy_bytes,
            overflowed: self.overflow.load(Ordering::Relaxed) || transport.overflowed,
        }
    }
    pub(crate) fn poll(&self, host_nanos: u64) {
        self.add(&self.polls, 1);
        self.poll_max.fetch_max(host_nanos, Ordering::Relaxed);
    }
    pub(crate) fn encode(&self, bytes: u32) {
        self.add(&self.codec, u64::from(bytes));
        self.codec_max
            .fetch_max(u64::from(bytes), Ordering::Relaxed);
    }
    pub(crate) fn wire(&self, bytes: usize) {
        self.add(&self.wire, bytes as u64);
    }
    pub(crate) fn staging(&self, bytes: u64) {
        self.add(&self.staging, bytes);
    }
    pub(crate) fn coalesced(&self, bytes: usize) {
        self.add(&self.coalesced, bytes as u64);
    }
    /// Shared passive transport counters, installed at connector construction.
    pub fn transport(&self) -> Arc<TransportTelemetry> {
        self.transport.clone()
    }
    /// Marks support before any TLS operation can be admitted.
    pub fn instrument_tls(&self) {
        self.transport.instrument_tls();
    }
    pub fn tls_write(&self, confirmed: usize) {
        self.transport.tls_write(confirmed);
    }
    pub fn tls_copy(&self, ciphertext: usize, plaintext: usize) {
        self.transport.tls_copy(ciphertext, plaintext);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overflow_is_visible_and_does_not_wrap_or_change_peaks() {
        let metrics = ProducerTelemetry::default();
        metrics.add(&metrics.wire, u64::MAX);
        metrics.wire(1);
        metrics.encode(128);
        metrics.encode(7);
        metrics.poll(64);
        metrics.poll(12);
        let s = metrics.snapshot();
        assert!(s.overflowed);
        assert_eq!(s.produce_wire_bytes_confirmed, u64::MAX);
        assert_eq!(s.codec_input_bytes, 135);
        assert_eq!(s.max_codec_quantum_bytes, 128);
        assert_eq!(s.max_host_poll_nanos, 64);
        assert_eq!(s.actor_polls, 2);
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    #[test]
    fn connector_transport_handle_and_legacy_methods_share_one_counter_bank() {
        let producer = ProducerTelemetry::default();
        let transport = producer.transport();
        assert!(Arc::ptr_eq(&transport, &producer.transport()));
        transport.instrument_tls();
        transport.tls_write(31);
        transport.tls_copy(7, 11);
        producer.tls_write(13);
        producer.tls_copy(2, 3);
        let snapshot = producer.snapshot();
        assert!(snapshot.tls_instrumented);
        assert_eq!(snapshot.tls_ciphertext_bytes_confirmed, 44);
        assert_eq!(snapshot.tls_ciphertext_copy_bytes, 9);
        assert_eq!(snapshot.tls_plaintext_copy_bytes, 14);
        assert_eq!(snapshot.produce_wire_bytes_confirmed, 0);
        assert!(!snapshot.overflowed);
    }
}
