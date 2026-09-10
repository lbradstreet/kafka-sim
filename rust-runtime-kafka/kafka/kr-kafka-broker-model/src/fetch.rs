//! Stateless, single-partition Fetch for raw producer round-trip probes.
use super::*;
use wire::{fetch_request, fetch_response, plan::Records};

impl BrokerModel {
    pub(super) fn fetch(
        &self,
        broker: i32,
        request: &fetch_request::v13::FetchRequest<'_>,
        correlation: i32,
        throttle: i32,
    ) -> Result<Vec<u8>> {
        use fetch_response::v13::{LeaderIdAndEpoch, PartitionData};
        if request.replica_id != -1
            || request.max_wait_ms != 0
            || request.min_bytes != 0
            || request.max_bytes <= 0
            || request.isolation_level != 0
            || request.session_id != 0
            || request.session_epoch != -1
            || !request.forgotten_topics_data.is_empty()
            || request.cluster_id.is_some()
            || !request.rack_id.is_empty()
            || request.topics.len() != 1
        {
            return Err(Error::InvalidRequest(
                "only stateless single-partition Fetch is supported",
            ));
        }
        let topic = request
            .topics
            .iter()
            .next()
            .ok_or(Error::InvalidRequest("missing Fetch topic"))??;
        if topic.partitions.len() != 1 {
            return Err(Error::InvalidRequest(
                "only one Fetch partition is supported",
            ));
        }
        let input = topic
            .partitions
            .iter()
            .next()
            .ok_or(Error::InvalidRequest("missing Fetch partition"))??;
        if input.last_fetched_epoch != -1
            || input.log_start_offset != -1
            || input.current_leader_epoch < -1
            || input.partition_max_bytes <= 0
        {
            return Err(Error::InvalidRequest("unsupported Fetch partition state"));
        }
        let mut output = PartitionData {
            partition_index: input.partition,
            high_watermark: -1,
            last_stable_offset: -1,
            log_start_offset: -1,
            aborted_transactions: None,
            ..Default::default()
        };
        let Some(topic_state) = self
            .topics
            .iter()
            .find(|state| !state.deleted && state.id == topic.topic_id)
        else {
            output.error_code = code::UNKNOWN_TOPIC_ID;
            return self.fetch_frame(topic.topic_id, output, correlation, throttle);
        };
        let Some(partition) = usize::try_from(input.partition)
            .ok()
            .and_then(|index| topic_state.partitions.get(index))
        else {
            output.error_code = code::UNKNOWN_TOPIC_OR_PARTITION;
            return self.fetch_frame(topic.topic_id, output, correlation, throttle);
        };
        output.high_watermark = partition.next_offset;
        output.last_stable_offset = partition.next_offset;
        output.log_start_offset = 0;
        output.current_leader = LeaderIdAndEpoch {
            leader_id: partition.leader,
            leader_epoch: partition.epoch,
            ..Default::default()
        };
        output.error_code = if broker != partition.leader {
            code::NOT_LEADER_OR_FOLLOWER
        } else if input.current_leader_epoch >= 0 && input.current_leader_epoch < partition.epoch {
            code::FENCED_LEADER_EPOCH
        } else if input.current_leader_epoch > partition.epoch {
            code::UNKNOWN_LEADER_EPOCH
        } else if input.fetch_offset < 0 || input.fetch_offset > partition.next_offset {
            code::OFFSET_OUT_OF_RANGE
        } else {
            0
        };
        if output.error_code != 0 {
            return self.fetch_frame(topic.topic_id, output, correlation, throttle);
        }
        // Compute the exact fixed frame size from the generated encoder. The
        // only size-changing field below is the compact record-set length.
        output.records = Some(Records::Borrowed(&[]));
        let empty = self.fetch_frame(topic.topic_id, output.clone(), correlation, throttle)?;
        let fixed_bytes = empty.len() - 1;
        drop(empty);
        let start = partition.batches.partition_point(|&index| {
            let batch = &self.log[index];
            batch.base_offset + batch.records.len() as i64 <= input.fetch_offset
        });
        let soft_limit = request.max_bytes.min(input.partition_max_bytes) as usize;
        let mut end = start;
        let mut total = 0usize;
        for &index in &partition.batches[start..] {
            let candidate = total
                .checked_add(self.wire_log[index].len())
                .ok_or(Error::Limit("Fetch record bytes"))?;
            if total != 0 && candidate > soft_limit {
                break;
            }
            // KIP-74 allows the first full batch past the request's soft byte
            // limit. The model's absolute encoded-frame bound remains strict.
            let frame_bytes = fixed_bytes
                .checked_add(compact_length_bytes(candidate)?)
                .and_then(|fixed| fixed.checked_add(candidate))
                .ok_or(Error::Limit("Fetch frame bytes"))?;
            if frame_bytes > self.config.max_frame_bytes {
                if total == 0 {
                    return Err(Error::Limit("first Fetch batch exceeds hard frame limit"));
                }
                break;
            }
            total = candidate;
            end += 1;
        }
        let mut records = Vec::new();
        records
            .try_reserve_exact(total)
            .map_err(|_| Error::Limit("Fetch response allocation"))?;
        for &index in &partition.batches[start..end] {
            records.extend_from_slice(&self.wire_log[index]);
        }
        output.records = Some(Records::Borrowed(&records));
        self.fetch_frame(topic.topic_id, output, correlation, throttle)
    }

    fn fetch_frame(
        &self,
        topic: TopicId,
        partition: fetch_response::v13::PartitionData<'_>,
        correlation: i32,
        throttle: i32,
    ) -> Result<Vec<u8>> {
        use fetch_response::v13::{FetchResponse, FetchableTopicResponse};
        let partitions = [partition];
        let topics = [FetchableTopicResponse {
            topic_id: topic,
            partitions: partitions.as_slice().into(),
            ..Default::default()
        }];
        self.encode(
            Response::FetchResponse(fetch_response::View::V13(FetchResponse {
                throttle_time_ms: throttle,
                responses: topics.as_slice().into(),
                ..Default::default()
            })),
            13,
            correlation,
        )
    }
}
fn compact_length_bytes(bytes: usize) -> Result<usize> {
    let mut length = u32::try_from(bytes)
        .ok()
        .and_then(|bytes| bytes.checked_add(1))
        .ok_or(Error::Limit("Fetch compact record length"))?;
    let mut size = 1;
    while length >= 128 {
        size += 1;
        length >>= 7;
    }
    Ok(size)
}
