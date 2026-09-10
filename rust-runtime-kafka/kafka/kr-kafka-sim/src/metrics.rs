//! Explicitly scheduled, reader-side HDR snapshots. The owner remains the only
//! histogram writer; every leased bank is summarized and dropped outside it.
use crate::ReplayManifest;
use kr_kafka_producer::{
    client::{ClientError, ProducerClient},
    telemetry::metrics::{Metric, MetricsReader, MetricsSnapshot, Scope, SnapshotError},
};
use kr_runtime::{JoinHandle, RuntimeHandle, RuntimeInstant};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    future::{Future, poll_fn},
    rc::Rc,
    task::{Poll, Waker},
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetricsSampling {
    pub interval_ns: u64,
}
impl MetricsSampling {
    pub(crate) fn validate(&self, m: &ReplayManifest) -> Result<(), String> {
        if !m.producer.metrics.enabled
            || self.interval_ns < 1_000_000
            || m.limits.elapsed_ns.div_ceil(self.interval_ns) + 2 > 1024
            || m.start_ns.checked_add(self.interval_ns).is_none()
            || m.producer
                .metrics
                .memory()
                .map_err(|e| e.to_string())?
                .configured_bytes
                > 512 * 1024 * 1024
        {
            return Err("metrics sampling interval/storage bounds".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetricScope {
    Global,
    Broker(i32),
    Partition { topic_id: [u8; 16], partition: i32 },
}
impl From<Scope> for MetricScope {
    fn from(scope: Scope) -> Self {
        match scope {
            Scope::Global => Self::Global,
            Scope::Broker(b) => Self::Broker(b),
            Scope::Partition {
                topic_id,
                partition,
            } => Self::Partition {
                topic_id,
                partition,
            },
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributionSample {
    /// Index in the producer's versioned Metric::ALL list.
    pub metric: u8,
    pub count: u64,
    pub p50: Option<[u64; 2]>,
    pub p90: Option<[u64; 2]>,
    pub p99: Option<[u64; 2]>,
    pub p999: Option<[u64; 2]>,
    pub exact_max: Option<u64>,
    pub significant_digits: u8,
    pub highest_trackable: u64,
    pub out_of_range: u64,
    pub count_overflow: u64,
    pub diagnostic_overflow: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScopeSample {
    pub scope: MetricScope,
    pub distributions: Vec<DistributionSample>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetricsSample {
    pub schema_version: u32,
    pub epoch: u64,
    pub requested_ns: Option<u64>,
    pub taken_ns: u64,
    pub start_ns: Option<u64>,
    pub end_ns: Option<u64>,
    pub scopes: Vec<ScopeSample>,
    pub omitted_scope_samples: u64,
    pub scope_capacity_rejections: u64,
    pub invalid_scope_samples: u64,
    pub invalid_time_samples: u64,
    pub missing_time_samples: u64,
    pub invalid_depth_samples: u64,
    pub diagnostic_overflow: bool,
}
impl MetricsSample {
    fn take(snapshot: &MetricsSnapshot, requested_ns: Option<u64>, taken_ns: u64) -> Self {
        let scopes = snapshot
            .scopes()
            .map(|scope| ScopeSample {
                scope: scope.into(),
                distributions: Metric::ALL
                    .into_iter()
                    .filter_map(|metric| {
                        let d = snapshot.distribution(scope, metric)?;
                        let q = |rank| d.quantile(rank).map(|range| [range.low, range.high]);
                        Some(DistributionSample {
                            metric: metric as u8,
                            count: d.count(),
                            p50: q(500_000),
                            p90: q(900_000),
                            p99: q(990_000),
                            p999: q(999_000),
                            exact_max: d.exact_max(),
                            significant_digits: d.significant_digits(),
                            highest_trackable: d.highest_trackable(),
                            out_of_range: d.out_of_range(),
                            count_overflow: d.count_overflow(),
                            diagnostic_overflow: d.diagnostic_overflow(),
                        })
                    })
                    .collect(),
            })
            .collect();
        Self {
            schema_version: snapshot.schema_version(),
            epoch: snapshot.epoch(),
            requested_ns,
            taken_ns,
            start_ns: snapshot.bounds().start.map(|t| t.as_nanos()),
            end_ns: snapshot.bounds().end.map(|t| t.as_nanos()),
            scopes,
            omitted_scope_samples: snapshot.omitted_scope_samples(),
            scope_capacity_rejections: snapshot.scope_capacity_rejections(),
            invalid_scope_samples: snapshot.invalid_scope_samples(),
            invalid_time_samples: snapshot.invalid_time_samples(),
            missing_time_samples: snapshot.missing_time_samples(),
            invalid_depth_samples: snapshot.invalid_depth_samples(),
            diagnostic_overflow: snapshot.diagnostic_overflow(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MissedMetricsRequest {
    pub scheduled_ns: u64,
    pub attempted_ns: u64,
    pub reason: String,
}
#[derive(Default)]
pub(crate) struct Samples {
    pub samples: Vec<MetricsSample>,
    pub missed: Vec<MissedMetricsRequest>,
    pub counts: [u64; Metric::COUNT],
    pending_request: Option<u64>,
}
impl Samples {
    pub(crate) fn collect(&mut self, reader: &MetricsReader, now: u64) -> Result<(), String> {
        while let Some(interval) = reader.try_take_snapshot() {
            if self.samples.len() == 1024 {
                return Err("metrics interval capacity".into());
            }
            if self
                .samples
                .last()
                .is_some_and(|last| last.epoch >= interval.epoch())
            {
                return Err("metrics epoch order".into());
            }
            let sample = MetricsSample::take(&interval, self.pending_request.take(), now);
            for metric in Metric::ALL {
                self.counts[metric as usize] = self.counts[metric as usize]
                    .checked_add(
                        interval
                            .distribution(Scope::Global, metric)
                            .map_or(0, |d| d.count()),
                    )
                    .ok_or("metrics aggregate count overflow")?;
            }
            self.samples
                .try_reserve(1)
                .map_err(|_| "metrics sample allocation")?;
            self.samples.push(sample);
            // Reset/recycle this bank in the reader task, before requesting the
            // next publication. Do not retain histogram storage in the report.
            drop(interval);
        }
        Ok(())
    }
    fn requested(
        &mut self,
        result: Result<(), ClientError>,
        scheduled_ns: u64,
        now: u64,
    ) -> Result<bool, String> {
        match result {
            Ok(()) => {
                self.pending_request = Some(now);
                Ok(true)
            }
            Err(ClientError::MetricsSnapshot(SnapshotError::Busy | SnapshotError::NoSpareBank)) => {
                if self.missed.len() == 1024 {
                    return Err("missed metrics request capacity".into());
                }
                self.missed.push(MissedMetricsRequest {
                    scheduled_ns,
                    attempted_ns: now,
                    reason: format!("{:?}", result.unwrap_err()),
                });
                Ok(true)
            }
            Err(ClientError::MetricsSnapshot(SnapshotError::Closed)) => Ok(false),
            Err(error) => Err(format!("metrics snapshot request: {error}")),
        }
    }
}
#[derive(Default)]
struct Stop {
    requested: bool,
    waker: Option<Waker>,
}
pub(crate) struct Sampler {
    stop: Rc<RefCell<Stop>>,
    task: Option<JoinHandle<Result<(), String>>>,
}
impl Sampler {
    pub(crate) fn spawn(
        handle: &RuntimeHandle,
        client: &ProducerClient,
        sampling: MetricsSampling,
        start_ns: u64,
        end: u64,
        output: Rc<RefCell<Samples>>,
    ) -> Result<Self, String> {
        let stop = Rc::new(RefCell::new(Stop::default()));
        let signal = stop.clone();
        let handle_run = handle.clone();
        let client = client.clone();
        let reader = client.metrics().map_err(|e| e.to_string())?;
        let mut next = start_ns
            .checked_add(sampling.interval_ns)
            .filter(|at| *at <= end);
        let task = handle
            .spawn(async move {
                loop {
                    output
                        .borrow_mut()
                        .collect(&reader, handle_run.now().as_nanos())?;
                    if reader.is_closed() {
                        break;
                    }
                    let mut timer = next
                        .map(|at| Box::pin(handle_run.sleep_until(RuntimeInstant::from_nanos(at))));
                    let tick = poll_fn(|cx| {
                        let mut signal = signal.borrow_mut();
                        if signal.requested {
                            return Poll::Ready(Ok(false));
                        }
                        signal.waker = Some(cx.waker().clone());
                        drop(signal);
                        match &mut timer {
                            Some(timer) => timer
                                .as_mut()
                                .poll(cx)
                                .map(|r| r.map(|()| true).map_err(|e| e.to_string())),
                            None => Poll::Pending,
                        }
                    })
                    .await?;
                    let now = handle_run.now().as_nanos();
                    output.borrow_mut().collect(&reader, now)?;
                    if !tick {
                        break;
                    }
                    let scheduled = next.expect("timer produced tick");
                    if !output.borrow_mut().requested(
                        client.request_metrics_snapshot(),
                        scheduled,
                        now,
                    )? {
                        break;
                    }
                    next = scheduled
                        .checked_add(sampling.interval_ns)
                        .filter(|at| *at <= end);
                }
                output
                    .borrow_mut()
                    .collect(&reader, handle_run.now().as_nanos())?;
                Ok(())
            })
            .map_err(|e| e.to_string())?;
        Ok(Self {
            stop,
            task: Some(task),
        })
    }
    pub(crate) async fn stop(mut self) -> Result<(), String> {
        let waker = {
            let mut stop = self.stop.borrow_mut();
            stop.requested = true;
            stop.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        self.task
            .take()
            .expect("owned sampler")
            .await
            .map_err(|e| e.to_string())?
    }
}
impl Drop for Sampler {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kr_kafka_producer::telemetry::metrics::{MetricsConfig, MetricsRecorder, ScopeToken};

    #[test]
    fn published_and_terminal_banks_are_summarized_once_with_diagnostics() {
        let mut recorder = MetricsRecorder::new(MetricsConfig {
            significant_digits: 2,
            highest_bytes: 1_000,
            max_partition_scopes: 2,
            max_storage_bytes: 64 * 1024 * 1024,
            ..MetricsConfig::default()
        })
        .unwrap();
        let reader = recorder.reader();
        let old = recorder.register_partition([1; 16], 0);
        let new = recorder.register_partition([2; 16], 0);
        recorder.observe_time(RuntimeInstant::from_nanos(10));
        recorder.record(Metric::BatchRawBytes, old, 997);
        reader.request_snapshot().unwrap();
        assert!(recorder.publish_at(RuntimeInstant::from_nanos(20)));
        recorder.record(Metric::BatchRawBytes, new, 2_000);
        recorder.record(Metric::BatchRawBytes, ScopeToken::GLOBAL, 5);
        recorder.observe_time(RuntimeInstant::from_nanos(30));
        drop(recorder);
        let mut samples = Samples::default();
        samples.requested(Ok(()), 18, 19).unwrap();
        samples.collect(&reader, 40).unwrap();
        samples.collect(&reader, 50).unwrap();
        assert_eq!(samples.samples.len(), 2);
        assert_eq!(samples.samples[0].requested_ns, Some(19));
        assert_eq!(samples.samples[1].requested_ns, None);
        assert_eq!(
            (samples.samples[0].start_ns, samples.samples[0].end_ns),
            (Some(10), Some(20))
        );
        assert_eq!(
            (samples.samples[1].start_ns, samples.samples[1].end_ns),
            (Some(20), Some(30))
        );
        let first = &samples.samples[0].scopes[0].distributions[Metric::BatchRawBytes as usize];
        assert_eq!(first.exact_max, Some(997));
        assert!(first.p99.unwrap()[0] <= 997 && first.p99.unwrap()[1] >= 997);
        let final_distribution =
            &samples.samples[1].scopes[0].distributions[Metric::BatchRawBytes as usize];
        assert_eq!(final_distribution.out_of_range, 1);
        assert_eq!(samples.counts[Metric::BatchRawBytes as usize], 2);
        assert!(samples.samples[1].scopes.iter().any(|s| s.scope
            == MetricScope::Partition {
                topic_id: [2; 16],
                partition: 0
            }));
    }

    #[test]
    fn snapshot_pressure_is_explicit_and_never_overwrites_the_pending_request() {
        let mut samples = Samples::default();
        samples.requested(Ok(()), 10, 11).unwrap();
        for error in [SnapshotError::Busy, SnapshotError::NoSpareBank] {
            assert!(
                samples
                    .requested(Err(ClientError::MetricsSnapshot(error)), 20, 21)
                    .unwrap()
            );
        }
        assert_eq!(samples.pending_request, Some(11));
        assert_eq!(samples.missed.len(), 2);
        assert!(
            !samples
                .requested(
                    Err(ClientError::MetricsSnapshot(SnapshotError::Closed)),
                    30,
                    31
                )
                .unwrap()
        );
        assert!(
            samples
                .requested(
                    Err(ClientError::MetricsSnapshot(SnapshotError::EpochExhausted)),
                    30,
                    31
                )
                .is_err()
        );
    }
}
