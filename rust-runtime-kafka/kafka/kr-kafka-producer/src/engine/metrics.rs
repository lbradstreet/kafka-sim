//! Passive observations at existing state transitions. No policy reads these
//! fields; per-record recording uses scope tokens stored by bounded creation.
use super::*;
use crate::telemetry::metrics::{
    Metric, MetricsConfig, MetricsReader, MetricsRecorder, ScopeToken,
};

pub(super) struct EngineMetrics {
    pub(super) recorder: MetricsRecorder,
    in_flight_bytes: Option<usize>,
}
impl EngineMetrics {
    pub(super) fn new(config: MetricsConfig) -> Result<Self> {
        Ok(Self {
            recorder: MetricsRecorder::new(config).map_err(|_| EngineError::AllocationFailed)?,
            in_flight_bytes: Some(0),
        })
    }
}
impl ProducerEngine {
    /// Independent read handle. Taking it does not request a snapshot or wake.
    pub fn metrics(&self) -> MetricsReader {
        self.metrics.recorder.reader()
    }
    /// Supplies the existing owner timestamp for otherwise untimed transitions.
    pub fn observe_metrics_time(&mut self, now: RuntimeInstant) {
        self.metrics.recorder.observe_time(now);
    }
    pub fn publish_metrics(&mut self, now: RuntimeInstant) -> bool {
        self.metrics.recorder.publish_at(now)
    }
    pub fn has_metrics_work(&self) -> bool {
        self.metrics.recorder.snapshot_requested()
    }
    pub(super) fn metrics_partition_scope(&self, partition: TopicPartition) -> ScopeToken {
        self.partitions
            .get(&partition)
            .map_or(ScopeToken::GLOBAL, |queue| queue.metrics_scope)
    }
    pub(super) fn metrics_queue_wait(
        &mut self,
        partition: TopicPartition,
        accepted: RuntimeInstant,
        now: RuntimeInstant,
    ) {
        let scope = self.metrics_partition_scope(partition);
        self.metrics
            .recorder
            .record_elapsed(Metric::QueueWaitNanos, scope, accepted, now);
    }
    pub(super) fn metrics_batch(&mut self, key: BatchKey) {
        let Some(batch) = self.batches.get_mut(key) else {
            return;
        };
        let partition = batch.partition();
        let seal = batch.take_seal_observation();
        let wire = batch.take_wire_observation();
        let scope = self.metrics_partition_scope(partition);
        let metrics = &mut self.metrics.recorder;
        if let Some(seal) = seal {
            if let Some(at) = seal.sealed_at {
                metrics.record_elapsed(Metric::BatchFillNanos, scope, seal.first_accepted, at);
            } else {
                metrics.missing_time();
            }
            metrics.record(Metric::BatchRawBytes, scope, u64::from(seal.raw_bytes));
            metrics.record(Metric::RecordsPerBatch, scope, seal.records as u64);
        }
        if let Some(bytes) = wire {
            metrics.record(Metric::BatchWireBytes, scope, bytes as u64);
        }
    }
    pub(super) fn metrics_delivery(
        &mut self,
        partition: TopicPartition,
        accepted: RuntimeInstant,
        kind: DeliveryKind,
    ) {
        let scope = self.metrics_partition_scope(partition);
        let metrics = &mut self.metrics.recorder;
        let metric = match kind {
            DeliveryKind::Acked => Metric::DeliveryAckedNanos,
            DeliveryKind::NotWritten => Metric::DeliveryNotWrittenNanos,
            DeliveryKind::Unknown => Metric::DeliveryUnknownNanos,
        };
        if let Some(now) = metrics.current_time() {
            metrics.record_elapsed(metric, scope, accepted, now);
        } else {
            metrics.missing_time();
        }
    }
    pub(super) fn metrics_request_rtt(
        &mut self,
        broker: i32,
        sent_at: Option<RuntimeInstant>,
        now: RuntimeInstant,
    ) {
        let scope = self
            .brokers
            .get(&broker)
            .map_or(ScopeToken::GLOBAL, |broker| broker.metrics_scope);
        if let Some(sent) = sent_at {
            self.metrics
                .recorder
                .record_elapsed(Metric::ProduceRttNanos, scope, sent, now);
        } else {
            self.metrics.recorder.missing_time();
        }
    }
    pub(super) fn metrics_request_depth(
        &mut self,
        broker: Option<i32>,
        bytes: usize,
        opened: bool,
    ) {
        self.metrics.in_flight_bytes = self.metrics.in_flight_bytes.and_then(|previous| {
            if opened {
                previous.checked_add(bytes)
            } else {
                previous.checked_sub(bytes)
            }
        });
        let recorder = &mut self.metrics.recorder;
        recorder.record(
            Metric::InFlightRequests,
            ScopeToken::GLOBAL,
            self.requests.len() as u64,
        );
        if let Some(bytes) = self.metrics.in_flight_bytes {
            recorder.record(Metric::InFlightWireBytes, ScopeToken::GLOBAL, bytes as u64);
        } else {
            recorder.invalid_depth();
        }
        if let Some(broker) = broker.and_then(|id| self.brokers.get(&id)) {
            recorder.record_scoped(
                Metric::InFlightRequests,
                broker.metrics_scope,
                broker.requests as u64,
            );
            recorder.record_scoped(
                Metric::InFlightWireBytes,
                broker.metrics_scope,
                broker.bytes as u64,
            );
        }
    }
}

#[cfg(test)]
mod tests;
