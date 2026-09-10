//! Explicit finite Fetch observation over the shared connector and driver.
//! Routing and the offset cursor belong only to this test harness.
use crate::{DomainEvent, RecordSpec, Workload, stream::ModelConnector};
use kr_kafka_client::{
    config::BrokerEndpoint,
    connector::{ConnectTarget, Connector},
    control::{ControlCodec, ControlLimits},
    fetch::{FetchRequest, FetchResponse},
    transport::{
        ConnectionDriver, DriverConfig, DriverEvent, OwnedSendPlan, RetireReason, SendRequest,
    },
    types::{TopicId, TopicPartition},
};
use kr_kafka_record::{BatchDecodeLimits, RecordSetIter, RecordSetLimits};
use kr_runtime::{RuntimeDuration, RuntimeHandle, RuntimeInstant};
use kr_runtime_io::network::ByteStreamVectoredSubmit;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::{Future, poll_fn},
    task::Poll,
};

const REQUEST_NS: u64 = 200_000_000;
const PROBE_NS: u64 = 2_000_000_000;
const FRAME_BYTES: usize = 1024 * 1024;

pub(crate) async fn verify(connector: &mut ModelConnector) -> Result<u64, String> {
    let probe_deadline = connector
        .handle
        .now()
        .checked_add(RuntimeDuration::from_nanos(PROBE_NS))
        .ok_or("Fetch probe deadline overflow")?;
    verify_inner(connector, probe_deadline).await
}

fn deadline(
    handle: &RuntimeHandle,
    probe_deadline: RuntimeInstant,
) -> Result<RuntimeInstant, String> {
    let now = handle.now();
    if now >= probe_deadline {
        return Err("Fetch probe aggregate virtual-time budget exceeded".into());
    }
    Ok(now
        .checked_add(RuntimeDuration::from_nanos(REQUEST_NS))
        .ok_or("Fetch deadline overflow")?
        .min(probe_deadline))
}

async fn verify_inner(
    connector: &mut ModelConnector,
    probe_deadline: RuntimeInstant,
) -> Result<u64, String> {
    let manifest = connector.manifest.clone();
    let expected: BTreeMap<_, _> = manifest
        .workload
        .iter()
        .filter_map(|op| match op {
            Workload::Submit { records } => Some(records),
            _ => None,
        })
        .flatten()
        .map(|record| (record.id, record))
        .collect();
    let mut committed: BTreeMap<TopicPartition, BTreeMap<i64, u64>> = BTreeMap::new();
    for batch in connector.model.borrow().log() {
        let partition = TopicPartition {
            topic: TopicId(batch.topic),
            partition: batch.partition,
        };
        for record in &batch.records {
            let id = crate::record_id(
                record
                    .headers
                    .iter()
                    .map(|h| (h.key.as_str(), h.value.as_deref())),
            )?;
            if committed
                .entry(partition)
                .or_default()
                .insert(record.offset, id)
                .is_some()
            {
                return Err("duplicate committed offset in Fetch reference".into());
            }
        }
    }
    let codec = ControlCodec::new("kr-dst-fetch-probe".into(), ControlLimits::default())
        .map_err(|e| e.to_string())?;
    let mut seen = BTreeSet::new();
    let mut correlation = 1_000_000i32;
    for topic in &manifest.topics {
        for partition_index in 0..topic.leaders.len() {
            let partition = TopicPartition {
                topic: TopicId(topic.id),
                partition: partition_index as i32,
            };
            let reference = committed.get(&partition).cloned().unwrap_or_default();
            let high_watermark = reference
                .last_key_value()
                .map_or(0, |(offset, _)| offset + 1);
            let (broker, epoch) = connector
                .model
                .borrow()
                .leader(topic.id, partition.partition)
                .map_err(|e| e.to_string())?;
            let endpoint = manifest
                .brokers
                .iter()
                .find(|entry| entry.id == broker)
                .ok_or("Fetch route broker absent")?;
            let mut offset = 0;
            let mut attempts = 0;
            let mut reached_end = false;
            while !reached_end {
                attempts += 1;
                if attempts > 8 {
                    return Err("Fetch probe connection retry bound".into());
                }
                let target = ConnectTarget {
                    endpoint: BrokerEndpoint {
                        host: endpoint.host.clone(),
                        port: endpoint.port,
                    },
                    broker_id: Some(broker),
                    lane: 0,
                    deadline: deadline(&connector.handle, probe_deadline)?,
                    driver: DriverConfig {
                        max_inflight_requests: 1,
                        ..DriverConfig::default()
                    },
                    lifetime_guard: None,
                };
                let Ok(mut connected) = connector.connect(target).await else {
                    continue;
                };
                let result: Result<bool, String> = async {
                    // A complete partition needs at most one batch per record,
                    // followed by one explicit empty end-offset observation.
                    for _ in 0..=manifest.limits.records {
                        correlation = correlation
                            .checked_add(1)
                            .ok_or("Fetch correlation overflow")?;
                        let request = FetchRequest {
                            partition,
                            offset,
                            current_leader_epoch: epoch,
                            max_bytes: 1024,
                        };
                        let frame = codec
                            .fetch13_request(correlation, &connected.capabilities, request)
                            .map_err(|e| e.to_string())?;
                        let deadline = deadline(&connector.handle, probe_deadline)?;
                        connected
                            .driver
                            .enqueue(SendRequest {
                                correlation,
                                deadline,
                                plan: OwnedSendPlan::from_frame(frame, FRAME_BYTES)
                                    .map_err(|e| e.to_string())?,
                            })
                            .map_err(|e| e.error.to_string())?;
                        let page = read_page(
                            &mut connected.driver,
                            &connector.handle,
                            &codec,
                            request,
                            correlation,
                            deadline,
                            |response| {
                                validate_page(
                                    response,
                                    request,
                                    high_watermark,
                                    &reference,
                                    &expected,
                                    &manifest.topics,
                                )
                            },
                        )
                        .await?;
                        let Some(page) = page else {
                            return Ok(false);
                        };
                        // No cursor or duplicate-set mutation occurs until the
                        // complete record set, including its tail, was checked.
                        if page.ids.iter().any(|id| seen.contains(id)) {
                            return Err("Fetch returned a duplicate workload ID".into());
                        }
                        seen.extend(page.ids.iter().copied());
                        connector.audit.borrow_mut().record(
                            connector.handle.now().as_nanos(),
                            DomainEvent::FetchVerified {
                                topic: topic.id,
                                partition: partition.partition,
                                from_offset: offset,
                                next_offset: page.next_offset,
                                high_watermark,
                                ids: page.ids,
                            },
                        );
                        if page.next_offset == offset {
                            if offset != high_watermark {
                                return Err("Fetch failed to advance below high watermark".into());
                            }
                            return Ok(true);
                        }
                        offset = page.next_offset;
                    }
                    Err("Fetch probe response-count bound".into())
                }
                .await;
                connected.driver.retire(RetireReason::Requested);
                release(&mut connected.driver, &connector.handle).await;
                reached_end = result?;
            }
        }
    }
    if seen.len() != expected.len() || expected.keys().any(|id| !seen.contains(id)) {
        return Err("Fetch observation did not recover every immutable workload record".into());
    }
    Ok(seen.len() as u64)
}

struct Page {
    next_offset: i64,
    ids: Vec<u64>,
}
fn validate_page(
    response: FetchResponse<'_>,
    request: FetchRequest,
    high_watermark: i64,
    reference: &BTreeMap<i64, u64>,
    expected: &BTreeMap<u64, &RecordSpec>,
    topics: &[crate::TopicSpec],
) -> Result<Page, String> {
    if response.error_code != 0
        || response.high_watermark != high_watermark
        || response.last_stable_offset != high_watermark
        || response.log_start_offset != 0
    {
        return Err("Fetch metadata differs from independent committed log".into());
    }
    let bytes = response.records.unwrap_or_default();
    let limits = RecordSetLimits {
        batch: BatchDecodeLimits {
            max_wire_bytes: FRAME_BYTES,
            max_raw_bytes: FRAME_BYTES,
            max_records: 256,
            max_headers: 4096,
            ..BatchDecodeLimits::default()
        },
        max_wire_bytes: FRAME_BYTES,
        max_raw_bytes: FRAME_BYTES,
        max_batches: 256,
        max_records: 256,
        max_headers: 4096,
    };
    let mut next_offset = request.offset;
    let mut ids = Vec::new();
    for batch in RecordSetIter::new(bytes, limits).map_err(|e| e.to_string())? {
        let batch = batch.map_err(|e| e.to_string())?;
        for record in batch.records() {
            let record = record.map_err(|e| e.to_string())?;
            let offset = batch
                .header
                .base_offset
                .checked_add(i64::from(record.offset_delta))
                .ok_or("Fetch record offset overflow")?;
            let headers = record
                .headers
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            let id = crate::record_id(headers.iter().map(|h| (h.key, h.value)))?;
            let expected = expected
                .get(&id)
                .ok_or("Fetch returned an unknown record")?;
            if reference.get(&offset) != Some(&id)
                || topics[expected.topic as usize].id != request.partition.topic.0
                || expected.partition != request.partition.partition
                || expected.key.as_deref() != record.key
                || expected.value.as_deref() != record.value
                || expected.timestamp_ms != record.timestamp
                || expected.headers.len() != headers.len()
                || expected
                    .headers
                    .iter()
                    .zip(&headers)
                    .any(|(a, b)| a.key != b.key || a.value.as_deref() != b.value)
            {
                return Err("Fetch record differs from workload or independent broker log".into());
            }
            if offset < request.offset {
                continue;
            }
            if offset != next_offset || offset >= high_watermark {
                return Err("Fetch offset order/gap violation".into());
            }
            next_offset = next_offset
                .checked_add(1)
                .ok_or("Fetch next offset overflow")?;
            ids.push(id);
        }
    }
    Ok(Page { next_offset, ids })
}

/// None is a transport interruption: reconnect with the same verified cursor.
async fn read_page<S: ByteStreamVectoredSubmit>(
    driver: &mut ConnectionDriver<S>,
    handle: &RuntimeHandle,
    codec: &ControlCodec,
    request: FetchRequest,
    correlation: i32,
    request_deadline: RuntimeInstant,
    validate: impl FnOnce(FetchResponse<'_>) -> Result<Page, String>,
) -> Result<Option<Page>, String> {
    let mut validate = Some(validate);
    let mut timer = Box::pin(
        handle.sleep(RuntimeDuration::from_nanos(
            request_deadline
                .as_nanos()
                .saturating_sub(handle.now().as_nanos()),
        )),
    );
    poll_fn(|cx| {
        let _ = timer.as_mut().poll(cx);
        for _ in 0..32 {
            match driver.poll_event(cx, handle.now()) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(DriverEvent::Frame { bytes, .. })) => {
                    return Poll::Ready(
                        codec
                            .parse_fetch13(bytes, correlation, request)
                            .map_err(|e| e.to_string())
                            .and_then(validate.take().unwrap())
                            .map(Some),
                    );
                }
                Poll::Ready(Some(
                    DriverEvent::Retiring { .. }
                    | DriverEvent::RequestRetired { .. }
                    | DriverEvent::Released,
                ))
                | Poll::Ready(None) => return Poll::Ready(Ok(None)),
                Poll::Ready(Some(
                    DriverEvent::WriteAdmitted { .. } | DriverEvent::WriteProgress { .. },
                )) => {}
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}
async fn release<S: ByteStreamVectoredSubmit>(
    driver: &mut ConnectionDriver<S>,
    handle: &RuntimeHandle,
) {
    poll_fn(|cx| {
        for _ in 0..32 {
            match driver.poll_event(cx, handle.now()) {
                Poll::Ready(Some(DriverEvent::Released)) | Poll::Ready(None) => {
                    return Poll::Ready(());
                }
                Poll::Ready(Some(_)) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use kr_kafka_record::{
        BatchConfig, CodecPool, Compression, EncodeBudget, Header, Identity, OutputPool,
        OwnedRecord, Record, RecordBatchBuilder, ZstdConfig,
    };

    fn batch(record: &RecordSpec, offset: i64) -> Vec<u8> {
        let config = BatchConfig {
            raw_limit: 4096,
            output_limit: 4096,
            chunk_bytes: 4096,
            progressive_threshold: 4096,
        };
        let pool = OutputPool::new(config.envelope_bytes() as usize).unwrap();
        let mut codecs = CodecPool::new(0, ZstdConfig::default()).unwrap();
        let mut builder = RecordBatchBuilder::new(config, Compression::None, pool).unwrap();
        let headers: Vec<_> = record
            .headers
            .iter()
            .map(|h| Header {
                key: &h.key,
                value: h.value.as_deref(),
            })
            .collect();
        builder
            .push(OwnedRecord::copy_from(Record {
                timestamp: record.timestamp_ms,
                key: record.key.as_deref(),
                value: record.value.as_deref(),
                headers: &headers,
            }))
            .unwrap();
        builder.request_seal().unwrap();
        for _ in 0..16 {
            builder
                .progress(
                    &mut codecs,
                    EncodeBudget {
                        input_bytes: 4096,
                        codec_calls: 1,
                    },
                )
                .unwrap();
            if let Some(batch) = builder.take_sealed() {
                let finalized = batch
                    .finalize(Identity {
                        producer_id: 1,
                        producer_epoch: 0,
                        base_sequence: offset as i32,
                    })
                    .unwrap();
                let mut bytes: Vec<_> = finalized
                    .chunks()
                    .iter()
                    .flat_map(|chunk| chunk.as_slice().iter().copied())
                    .collect();
                bytes[..8].copy_from_slice(&offset.to_be_bytes());
                return bytes;
            }
        }
        panic!("record fixture failed to seal");
    }
    #[test]
    fn corrupt_tail_cannot_return_a_verified_prefix_or_advance_the_probe_cursor() {
        let topic = crate::TopicSpec {
            id: [7; 16],
            name: "probe".into(),
            leaders: vec![1],
        };
        let records: Vec<_> = (1u64..=2)
            .map(|id| RecordSpec {
                id,
                topic: 0,
                partition: 0,
                key_routed: false,
                lane: 0,
                key: Some(id.to_be_bytes().to_vec()),
                value: None,
                timestamp_ms: id as i64,
                native: false,
                headers: crate::identity_headers(id),
            })
            .collect();
        let expected: BTreeMap<_, _> = records.iter().map(|r| (r.id, r)).collect();
        let reference = BTreeMap::from([(0, 1), (1, 2)]);
        let request = FetchRequest {
            partition: TopicPartition {
                topic: TopicId(topic.id),
                partition: 0,
            },
            offset: 0,
            current_leader_epoch: -1,
            max_bytes: 1024,
        };
        let mut wire = batch(&records[0], 0);
        let split = wire.len();
        wire.extend_from_slice(&batch(&records[1], 1));
        let check = |bytes| {
            validate_page(
                FetchResponse {
                    partition: request.partition,
                    error_code: 0,
                    throttle_ms: 0,
                    high_watermark: 2,
                    last_stable_offset: 2,
                    log_start_offset: 0,
                    current_leader: None,
                    current_leader_epoch: None,
                    preferred_read_replica: None,
                    records: Some(bytes),
                },
                request,
                2,
                &reference,
                &expected,
                std::slice::from_ref(&topic),
            )
        };
        let valid = check(&wire).unwrap();
        assert_eq!(valid.ids, vec![1, 2]);
        assert_eq!(valid.next_offset, 2);
        let mut corrupt = wire.clone();
        corrupt[split + 17] ^= 1;
        assert!(check(&corrupt).err().unwrap().contains("Checksum"));
        assert!(check(&wire[..wire.len() - 1]).is_err());
        assert_eq!(request.offset, 0);
    }
}
