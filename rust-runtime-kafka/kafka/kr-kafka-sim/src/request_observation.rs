//! Thread-safe, bounded staging sink for passive client hooks. Audit drains it
//! before its next observation, translating workload IDs into accepted tokens.
use crate::{DomainEvent, ReplayManifest};
use kr_kafka_client::transport::{RequestFinish, RequestObservation, RequestObserver};
use kr_kafka_protocol::{
    Request,
    frame::decode_request,
    wire::{DecodeLimits, Records},
};
use kr_kafka_record::{BatchDecodeLimits, inspect_batch};
use kr_runtime::RuntimeInstant;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DispatchBatch {
    /// Harness identity for this immutable topic/partition token cohort.
    pub batch_id: u64,
    pub topic: [u8; 16],
    pub partition: i32,
    pub records: u32,
    /// Encoded record payload before compression, excluding the 61-byte header.
    pub raw_bytes: u64,
    /// Complete batch bytes on the wire, including the fixed batch header.
    pub wire_bytes: u64,
}
type BatchKey = ([u8; 16], i32, Vec<u64>);
struct State {
    next_request: u64,
    cohort_tokens: usize,
    batches: BTreeMap<BatchKey, u64>,
    active: BTreeMap<(u64, i32), u64>,
    queued: Vec<(u64, DomainEvent)>,
    error: Option<String>,
}
pub(crate) struct Capture {
    maximum: usize,
    max_batches: usize,
    max_active: usize,
    frame_bytes: usize,
    batch_limits: BatchDecodeLimits,
    state: Mutex<State>,
    pending: AtomicBool,
}
impl Capture {
    pub(crate) fn new(manifest: &ReplayManifest) -> Arc<Self> {
        Arc::new(Self {
            maximum: manifest.limits.history_events,
            max_batches: manifest.limits.records as usize * 4,
            max_active: manifest.network.connections * 256,
            frame_bytes: manifest.model.frame_bytes,
            batch_limits: manifest
                .model
                .config(manifest.produce_max_version)
                .batch_limits,
            pending: AtomicBool::new(false),
            state: Mutex::new(State {
                next_request: 1,
                cohort_tokens: 0,
                batches: BTreeMap::new(),
                active: BTreeMap::new(),
                queued: Vec::new(),
                error: None,
            }),
        })
    }
    pub(crate) fn connection(self: &Arc<Self>, connection: u64) -> Arc<dyn RequestObserver> {
        Arc::new(ConnectionCapture {
            connection,
            capture: self.clone(),
        })
    }
    pub(crate) fn drain_into(&self, spare: &mut Vec<(u64, DomainEvent)>) -> Result<(), String> {
        debug_assert!(spare.is_empty());
        if !self.pending.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| "request observation lock poisoned")?;
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        std::mem::swap(spare, &mut state.queued);
        self.pending.store(false, Ordering::Release);
        Ok(())
    }
    pub(crate) fn finish(&self) -> Result<(), String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "request observation lock poisoned")?;
        if let Some(error) = &state.error {
            return Err(error.clone());
        }
        if !state.active.is_empty() {
            return Err("client observation retained unfinished requests".into());
        }
        Ok(())
    }
    fn observe(
        &self,
        connection: u64,
        now: u64,
        transition: RequestObservation<'_>,
        state: &mut State,
    ) -> Result<(), String> {
        if state.queued.len() == self.maximum {
            return Err("client observation queue capacity".into());
        }
        state
            .queued
            .try_reserve(1)
            .map_err(|_| "client observation queue allocation")?;
        let event = match transition {
            RequestObservation::Dispatched { correlation, plan } => {
                if state.active.len() == self.max_active
                    || state.active.contains_key(&(connection, correlation))
                {
                    return Err("client request identity/capacity".into());
                }
                if plan.len() > self.frame_bytes {
                    return Err("observed request frame capacity".into());
                }
                let mut bytes = Vec::new();
                bytes
                    .try_reserve_exact(plan.len())
                    .map_err(|_| "observed request allocation")?;
                for segment in plan.segments() {
                    bytes.extend_from_slice(segment.as_slice());
                }
                let frame =
                    decode_request(&bytes, DecodeLimits::default()).map_err(|e| e.to_string())?;
                if frame.correlation_id != correlation {
                    return Err("observed dispatch correlation mismatch".into());
                }
                let mut ids = Vec::new();
                let mut batches = Vec::new();
                if let Request::ProduceRequest(kr_kafka_protocol::produce_request::View::V13(
                    request,
                )) = &frame.body
                {
                    for topic in request.topic_data.iter() {
                        let topic = topic.map_err(|e| e.to_string())?;
                        for partition in topic.partition_data.iter() {
                            let partition = partition.map_err(|e| e.to_string())?;
                            let Some(Records::Borrowed(bytes)) = partition.records else {
                                return Err("observed produce records missing".into());
                            };
                            let batch = inspect_batch(bytes, self.batch_limits)
                                .map_err(|e| e.to_string())?;
                            let before = ids.len();
                            for record in batch.records() {
                                let record = record.map_err(|e| e.to_string())?;
                                let headers = record
                                    .headers
                                    .collect::<Result<Vec<_>, _>>()
                                    .map_err(|e| e.to_string())?;
                                ids.push(crate::manifest::record_id(
                                    headers.iter().map(|h| (h.key, h.value)),
                                )?);
                            }
                            let records = u32::try_from(ids.len() - before)
                                .map_err(|_| "observed batch record count")?;
                            if records == 0 {
                                return Err("empty observed batch".into());
                            }
                            let key = (topic.topic_id, partition.index, ids[before..].to_vec());
                            let batch_id = match state.batches.get(&key) {
                                Some(id) => *id,
                                None => {
                                    let count = state
                                        .cohort_tokens
                                        .checked_add(records as usize)
                                        .ok_or("observed cohort token overflow")?;
                                    if count > self.max_batches
                                        || state.batches.len() == self.max_batches
                                    {
                                        return Err("observed batch identity capacity".into());
                                    }
                                    let id = state.batches.len() as u64 + 1;
                                    state.batches.insert(key, id);
                                    state.cohort_tokens = count;
                                    id
                                }
                            };
                            batches.push(DispatchBatch {
                                batch_id,
                                topic: topic.topic_id,
                                partition: partition.index,
                                records,
                                raw_bytes: batch.raw_bytes().len() as u64,
                                wire_bytes: bytes.len() as u64,
                            });
                        }
                    }
                } else if frame.api_key == 0 {
                    return Err("observed unsupported Produce layout".into());
                }
                let request_id = state.next_request;
                state.next_request = request_id
                    .checked_add(1)
                    .ok_or("observed request ID overflow")?;
                state.active.insert((connection, correlation), request_id);
                DomainEvent::ClientRequestDispatched {
                    connection,
                    correlation,
                    api: frame.api_key,
                    request_id,
                    tokens: ids,
                    wire_bytes: plan.len() as u64,
                    batches,
                }
            }
            RequestObservation::WriteCompleted { correlation } => {
                let request_id = *state
                    .active
                    .get(&(connection, correlation))
                    .ok_or("unobserved write completion")?;
                DomainEvent::ClientRequestWriteCompleted {
                    request_id,
                    connection,
                    correlation,
                }
            }
            RequestObservation::Finished {
                correlation,
                dispatched,
                confirmed,
                certainty,
                result,
            } => {
                if !dispatched {
                    return Ok(());
                }
                let request_id = state
                    .active
                    .remove(&(connection, correlation))
                    .ok_or("unobserved request finish")?;
                DomainEvent::ClientRequestFinished {
                    request_id,
                    connection,
                    correlation,
                    confirmed: confirmed as u64,
                    result: match result {
                        RequestFinish::Response => "Response".into(),
                        RequestFinish::Retired(reason) => format!("Retired:{reason:?}"),
                    },
                    certainty: format!("{certainty:?}"),
                }
            }
        };
        state.queued.push((now, event));
        Ok(())
    }
}
struct ConnectionCapture {
    connection: u64,
    capture: Arc<Capture>,
}
impl RequestObserver for ConnectionCapture {
    fn observe(&self, now: RuntimeInstant, transition: RequestObservation<'_>) {
        if let Ok(mut state) = self.capture.state.lock() {
            if state.error.is_some() {
                return;
            }
            if let Err(error) =
                self.capture
                    .observe(self.connection, now.as_nanos(), transition, &mut state)
            {
                state.error = Some(error);
            }
            self.capture.pending.store(true, Ordering::Release);
        } else {
            self.capture.pending.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kr_kafka_client::transport::OwnedSendPlan;
    use kr_kafka_producer::control::{ControlCodec, Probe};

    #[test]
    fn bounded_capture_reports_overflow_without_panicking_or_retaining_the_plan() {
        let manifest = ReplayManifest::from_seed(0, crate::CampaignLimits::default()).unwrap();
        let mut capture = Capture::new(&manifest);
        Arc::get_mut(&mut capture).unwrap().maximum = 1;
        let observer = capture.connection(1);
        let frame = ControlCodec::from_config(&manifest.producer)
            .unwrap()
            .api_versions_request(-1, Probe::V3)
            .unwrap();
        let plan = OwnedSendPlan::from_frame(frame, manifest.model.frame_bytes).unwrap();
        observer.observe(
            RuntimeInstant::ZERO,
            RequestObservation::Dispatched {
                correlation: -1,
                plan: &plan,
            },
        );
        observer.observe(
            RuntimeInstant::ZERO,
            RequestObservation::WriteCompleted { correlation: -1 },
        );
        drop(plan);
        assert!(
            capture
                .drain_into(&mut Vec::new())
                .unwrap_err()
                .contains("queue capacity")
        );
        assert!(capture.finish().is_err());
    }

    #[test]
    fn malformed_frame_is_a_diagnostic_failure_and_empty_drains_reuse_storage() {
        let manifest = ReplayManifest::from_seed(0, crate::CampaignLimits::default()).unwrap();
        let capture = Capture::new(&manifest);
        let observer = capture.connection(1);
        let mut spare = Vec::with_capacity(4);
        let pointer = spare.as_ptr();
        capture.drain_into(&mut spare).unwrap();
        assert_eq!(spare.as_ptr(), pointer);
        let plan = OwnedSendPlan::from_frame(vec![0, 0, 0, 4, 0, 0, 0, 0], 128).unwrap();
        observer.observe(
            RuntimeInstant::ZERO,
            RequestObservation::Dispatched {
                correlation: 0,
                plan: &plan,
            },
        );
        assert!(capture.drain_into(&mut spare).is_err());
        assert!(capture.finish().is_err());
    }
}
