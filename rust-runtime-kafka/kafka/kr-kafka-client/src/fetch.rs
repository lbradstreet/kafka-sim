//! One stateless Fetch v13 operation addressed by topic UUID and partition.
//!
//! This is a checked wire boundary. The caller owns routing, the explicit fetch
//! offset, the connection, deadlines, and any retry decision. Responses borrow
//! opaque record-set bytes; this crate neither decompresses nor consumes them.
use crate::{
    control::{Capabilities, ControlCodec, ControlError, Result, support::throttle},
    types::TopicPartition,
};
use kr_kafka_protocol::{
    Request, Response, fetch_request, fetch_response,
    wire::{Records, Sequence},
};

/// Fetch exactly one partition, without establishing an incremental session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchRequest {
    pub partition: TopicPartition,
    /// Explicit nonnegative log offset; no earliest/latest offset lookup occurs.
    pub offset: i64,
    /// `-1` means the caller does not know the current leader epoch.
    pub current_leader_epoch: i32,
    /// Applied to both the request and its single partition. Kafka may return
    /// an oversized first batch for progress; the codec's frame cap is the hard
    /// input bound and must allow the caller's largest supported batch.
    pub max_bytes: u32,
}
impl FetchRequest {
    fn validate(self) -> Result<()> {
        if self.partition.topic.is_zero() || self.partition.partition < 0 {
            return Err(ControlError::Invalid("fetch partition identity"));
        }
        if self.offset < 0 || self.current_leader_epoch < -1 {
            return Err(ControlError::Invalid("fetch offset or leader epoch"));
        }
        if self.max_bytes == 0 || self.max_bytes > i32::MAX as u32 {
            return Err(ControlError::Invalid("fetch byte allowance"));
        }
        Ok(())
    }
}

/// Fully framed and identity-checked response. Offset fields retain Kafka's
/// `-1` unavailable sentinel; a successful partition has a known high watermark.
/// Leader ID and epoch are independently optional, including partial hints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchResponse<'a> {
    pub partition: TopicPartition,
    pub error_code: i16,
    pub throttle_ms: u32,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub current_leader: Option<i32>,
    pub current_leader_epoch: Option<i32>,
    pub preferred_read_replica: Option<i32>,
    /// Null and empty stay distinct. Payload bytes are not parsed or copied.
    pub records: Option<&'a [u8]>,
}

impl ControlCodec {
    /// Encodes Fetch 13 only after validating the advertised capability. Fields
    /// are fixed to a normal read-uncommitted, immediate, sessionless request:
    /// replica -1, max-wait/min-bytes 0, session 0/epoch -1, no forgotten topics,
    /// no rack preference, and no last-fetched epoch or follower log-start hint.
    pub fn fetch13_request(
        &self,
        correlation: i32,
        capabilities: &Capabilities,
        request: FetchRequest,
    ) -> Result<Vec<u8>> {
        request.validate()?;
        capabilities.require(1, 13)?;
        let partitions = [fetch_request::v13::FetchPartition {
            partition: request.partition.partition,
            current_leader_epoch: request.current_leader_epoch,
            fetch_offset: request.offset,
            partition_max_bytes: request.max_bytes as i32,
            last_fetched_epoch: -1,
            log_start_offset: -1,
            ..Default::default()
        }];
        let topics = [fetch_request::v13::FetchTopic {
            topic_id: request.partition.topic.0,
            partitions: Sequence::from_slice(&partitions),
            ..Default::default()
        }];
        self.encode_request(
            Request::FetchRequest(fetch_request::View::V13(fetch_request::v13::FetchRequest {
                replica_id: -1,
                max_wait_ms: 0,
                min_bytes: 0,
                max_bytes: request.max_bytes as i32,
                isolation_level: 0,
                session_id: 0,
                session_epoch: -1,
                topics: Sequence::from_slice(&topics),
                forgotten_topics_data: Sequence::from_slice(&[]),
                rack_id: "",
                ..Default::default()
            })),
            13,
            correlation,
        )
    }

    /// Validates the complete frame and exact expected UUID/partition before
    /// exposing borrowed bytes. No metadata cache, offset or retry state changes.
    /// Top-level broker errors return `ControlError::Broker`; a partition error
    /// is returned with its checked partition identity in `FetchResponse`.
    pub fn parse_fetch13<'a>(
        &self,
        bytes: &'a [u8],
        correlation: i32,
        expected: FetchRequest,
    ) -> Result<FetchResponse<'a>> {
        expected.validate()?;
        let frame = self.decode_response(bytes, 1, 13, correlation)?;
        let Response::FetchResponse(fetch_response::View::V13(response)) = frame.body else {
            return Err(ControlError::UnexpectedBody);
        };
        let throttle_ms = throttle(response.throttle_time_ms)?;
        if response.session_id != 0 {
            return Err(ControlError::Invalid("unexpected fetch session"));
        }
        if response.error_code != 0 {
            return Err(ControlError::Broker {
                api_key: 1,
                error_code: response.error_code,
            });
        }
        if response.responses.len() != 1 {
            return Err(ControlError::UnexpectedTopic);
        }
        let topic = response
            .responses
            .iter()
            .next()
            .ok_or(ControlError::UnexpectedTopic)??;
        if topic.topic_id != expected.partition.topic.0 {
            return Err(ControlError::IdentityChanged);
        }
        if topic.partitions.len() != 1 {
            return Err(ControlError::UnexpectedPartition);
        }
        let partition = topic
            .partitions
            .iter()
            .next()
            .ok_or(ControlError::UnexpectedPartition)??;
        if partition.partition_index != expected.partition.partition {
            return Err(ControlError::UnexpectedPartition);
        }
        if partition.high_watermark < -1
            || (partition.error_code == 0 && partition.high_watermark < 0)
            || partition.last_stable_offset < -1
            || partition.log_start_offset < -1
            || partition.current_leader.leader_id < -1
            || partition.current_leader.leader_epoch < -1
            || partition.preferred_read_replica < -1
            || partition.diverging_epoch.epoch < -1
            || partition.diverging_epoch.end_offset < -1
            || partition.snapshot_id.epoch < -1
            || partition.snapshot_id.end_offset < -1
        {
            return Err(ControlError::Invalid(
                "fetch response offset or leader hint",
            ));
        }
        if let Some(aborted) = &partition.aborted_transactions {
            for transaction in aborted.iter() {
                let transaction = transaction?;
                if transaction.producer_id < 0 || transaction.first_offset < 0 {
                    return Err(ControlError::Invalid("fetch aborted transaction"));
                }
            }
        }
        let records = match partition.records {
            None => None,
            Some(Records::Borrowed(bytes)) => Some(bytes),
            Some(_) => return Err(ControlError::UnexpectedBody),
        };
        Ok(FetchResponse {
            partition: expected.partition,
            error_code: partition.error_code,
            throttle_ms,
            high_watermark: partition.high_watermark,
            last_stable_offset: partition.last_stable_offset,
            log_start_offset: partition.log_start_offset,
            current_leader: optional(partition.current_leader.leader_id),
            current_leader_epoch: optional(partition.current_leader.leader_epoch),
            preferred_read_replica: optional(partition.preferred_read_replica),
            records,
        })
    }
}
fn optional(value: i32) -> Option<i32> {
    (value >= 0).then_some(value)
}

#[cfg(test)]
mod tests;
