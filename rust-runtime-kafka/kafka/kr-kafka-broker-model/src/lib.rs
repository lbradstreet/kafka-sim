//! A bounded passive broker cluster for deterministic producer testing.
//!
//! All input and output are actual Kafka frames. Time, connection lifetime,
//! partial I/O, and delivery scheduling belong to the driving runtime actor.
#![no_std]
#![forbid(unsafe_code)]
extern crate alloc;
mod fetch;
mod metadata;
mod oracle;
pub use oracle::{
    AcceptedRecord, CreditObservation, DeliveryOracle, ObservedDelivery, ObservedOutcome,
    ObservedResponse, OracleLimits, OracleReport, Violation,
};
mod produce;

use alloc::{
    collections::VecDeque,
    string::{String, ToString},
    vec::Vec,
};
use kr_kafka_protocol::{
    self as wire, Request, Response, errors as code,
    frame::decode_request,
    wire::{DecodeLimits, EncodeLimits},
};
use kr_kafka_record::{BatchDecodeLimits, Identity};
pub type TopicId = [u8; 16];
#[derive(Clone, Debug)]
pub struct BrokerConfig {
    pub produce_max_version: i16,
    pub max_frame_bytes: usize,
    pub max_topics: usize,
    pub max_partitions: usize,
    pub max_brokers: usize,
    pub max_producers: usize,
    pub max_log_batches: usize,
    pub max_log_records: usize,
    /// Logical decoded batch bytes plus retained original wire allocation
    /// capacity. Container/header allocations have separate cardinality bounds.
    pub max_log_bytes: usize,
    pub max_string_bytes: usize,
    pub topic_id_prefix: u64,
    pub batch_limits: BatchDecodeLimits,
}
impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            produce_max_version: 13,
            max_frame_bytes: 4 * 1024 * 1024,
            max_topics: 128,
            max_partitions: 1024,
            max_brokers: 16,
            max_producers: 64,
            max_log_batches: 65536,
            max_log_records: 1_000_000,
            max_log_bytes: 64 * 1024 * 1024,
            max_string_bytes: 1024,
            topic_id_prefix: 0x4b61666b614d6f64,
            batch_limits: BatchDecodeLimits::default(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerEndpoint {
    pub id: i32,
    pub host: String,
    pub port: u16,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaderMove {
    pub topic: TopicId,
    pub partition: i32,
    pub broker: i32,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FaultPlan {
    pub drop_after_parse: bool,
    pub reject_before_commit: Option<i16>,
    pub drop_after_commit: bool,
    pub disconnect_before_response: bool,
    pub throttle_time_ms: i32,
    pub leader_move: Option<LeaderMove>,
    /// Return Kafka's explicit duplicate code instead of the usual original
    /// successful offset when a retained sequence range is retried.
    pub duplicate_sequence_error: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrokerAction {
    Reply(Vec<u8>),
    DropRequest,
    DropResponse { committed_batches: usize },
    Disconnect { committed_batches: usize },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig,
    UnknownBroker,
    UnknownTopic,
    UnknownPartition,
    DuplicateTopic,
    DuplicateBroker,
    Limit(&'static str),
    InvalidRequest(&'static str),
    Wire(wire::wire::Error),
}
impl From<wire::wire::Error> for Error {
    fn from(error: wire::wire::Error) -> Self {
        Self::Wire(error)
    }
}
impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl core::error::Error for Error {}
pub type Result<T> = core::result::Result<T, Error>;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedRecord {
    pub offset: i64,
    pub sequence: i32,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<CommittedHeader>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedBatch {
    pub topic: TopicId,
    pub partition: i32,
    pub identity: Identity,
    pub base_offset: i64,
    pub records: Vec<CommittedRecord>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BrokerStats {
    pub requests: u64,
    pub dropped_requests: u64,
    pub dropped_responses: u64,
    pub committed_batches: usize,
    pub committed_records: usize,
    pub log_bytes: usize,
    pub decoded_log_bytes: usize,
    pub wire_log_bytes: usize,
    pub duplicate_batches: u64,
    pub rejected_batches: u64,
    pub leader_moves: u64,
    pub topic_deletions: u64,
    pub topic_creations: u64,
}
struct Topic {
    id: TopicId,
    name: String,
    deleted: bool,
    partitions: Vec<Partition>,
}
struct Partition {
    leader: i32,
    epoch: i32,
    next_offset: i64,
    producers: Vec<ProducerState>,
    /// Global log indexes in offset order, enabling bounded binary search.
    batches: Vec<usize>,
}
struct ProducerState {
    id: i64,
    epoch: i16,
    next_sequence: i32,
    history: VecDeque<BatchHistory>,
}
struct BatchHistory {
    base_sequence: i32,
    count: i32,
    base_offset: i64,
}
/// Cluster state. Topic capacity counts deleted IDs too, so a recreate campaign
/// is bounded and historical logs remain inspectable without name rebinding.
pub struct BrokerModel {
    config: BrokerConfig,
    brokers: Vec<BrokerEndpoint>,
    topics: Vec<Topic>,
    next_topic: u64,
    next_pid: i64,
    issued_producers: usize,
    log: Vec<CommittedBatch>,
    wire_log: Vec<Vec<u8>>,
    stats: BrokerStats,
}
impl BrokerModel {
    pub fn new(config: BrokerConfig) -> Result<Self> {
        if !matches!(config.produce_max_version, 9 | 13)
            || config.max_frame_bytes < 64
            || config.max_frame_bytes > i32::MAX as usize
            || config.max_topics == 0
            || config.max_partitions == 0
            || config.max_partitions > i32::MAX as usize
            || config.max_brokers == 0
            || config.max_producers == 0
            || config.max_log_batches == 0
            || config.max_log_records == 0
            || config.max_log_bytes == 0
            || config.max_string_bytes == 0
        {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            config,
            brokers: Vec::new(),
            topics: Vec::new(),
            next_topic: 1,
            next_pid: 1,
            issued_producers: 0,
            log: Vec::new(),
            wire_log: Vec::new(),
            stats: BrokerStats::default(),
        })
    }
    pub fn config(&self) -> &BrokerConfig {
        &self.config
    }
    pub fn stats(&self) -> BrokerStats {
        self.stats
    }
    pub fn log(&self) -> &[CommittedBatch] {
        &self.log
    }
    /// Original committed magic-2 bytes, indexed like `log()`. Only base offset
    /// and leader epoch are patched at append, outside the checksum region.
    pub fn wire_batch(&self, index: usize) -> Option<&[u8]> {
        self.wire_log.get(index).map(Vec::as_slice)
    }
    pub fn brokers(&self) -> &[BrokerEndpoint] {
        &self.brokers
    }
    pub fn add_broker(&mut self, broker: BrokerEndpoint) -> Result<()> {
        if broker.id < 0
            || broker.host.is_empty()
            || broker.port == 0
            || broker.host.len() > self.config.max_string_bytes
        {
            return Err(Error::InvalidConfig);
        }
        if self.brokers.iter().any(|b| b.id == broker.id) {
            return Err(Error::DuplicateBroker);
        }
        if self.brokers.len() == self.config.max_brokers {
            return Err(Error::Limit("brokers"));
        }
        self.brokers.push(broker);
        Ok(())
    }
    pub fn create_topic(&mut self, name: &str, leaders: &[i32]) -> Result<TopicId> {
        let mut id = [0; 16];
        id[..8].copy_from_slice(&self.config.topic_id_prefix.to_be_bytes());
        id[8..].copy_from_slice(&self.next_topic.to_be_bytes());
        let next = self
            .next_topic
            .checked_add(1)
            .ok_or(Error::Limit("topic IDs"))?;
        self.create_topic_with_id(name, id, leaders)?;
        self.next_topic = next;
        Ok(id)
    }
    pub fn create_topic_with_id(&mut self, name: &str, id: TopicId, leaders: &[i32]) -> Result<()> {
        if name.is_empty()
            || name.len() > self.config.max_string_bytes
            || id == [0; 16]
            || leaders.is_empty()
        {
            return Err(Error::InvalidConfig);
        }
        if self
            .topics
            .iter()
            .any(|t| t.id == id || (!t.deleted && t.name == name))
        {
            return Err(Error::DuplicateTopic);
        }
        if self.topics.len() == self.config.max_topics {
            return Err(Error::Limit("topics"));
        }
        self.validate_partition_growth(leaders)?;
        self.topics.push(Topic {
            id,
            name: name.to_string(),
            deleted: false,
            partitions: leaders
                .iter()
                .map(|&leader| Partition {
                    leader,
                    epoch: 0,
                    next_offset: 0,
                    producers: Vec::new(),
                    batches: Vec::new(),
                })
                .collect(),
        });
        self.stats.topic_creations += 1;
        Ok(())
    }
    fn validate_partition_growth(&self, leaders: &[i32]) -> Result<()> {
        if leaders.len()
            > self.config.max_partitions
                - self
                    .topics
                    .iter()
                    .map(|t| t.partitions.len())
                    .sum::<usize>()
        {
            return Err(Error::Limit("partitions"));
        }
        if leaders
            .iter()
            .any(|leader| !self.brokers.iter().any(|b| b.id == *leader))
        {
            return Err(Error::UnknownBroker);
        }
        Ok(())
    }
    pub fn delete_topic(&mut self, id: TopicId) -> Result<()> {
        let topic = self
            .topics
            .iter_mut()
            .find(|t| t.id == id)
            .ok_or(Error::UnknownTopic)?;
        if !topic.deleted {
            topic.deleted = true;
            self.stats.topic_deletions += 1;
        }
        Ok(())
    }
    /// Append exactly `additional_leaders.len()` partitions to a live topic.
    ///
    /// The supplied leaders describe only the new partitions, whose contiguous
    /// IDs start at the previous partition count. Each starts with leader epoch
    /// and next offset zero and no producer state. Existing partitions and their
    /// state remain unchanged. An empty slice is a no-op for a live topic.
    ///
    /// The topic ID, every leader, and the cluster-wide partition limit
    /// (including deleted topics) are validated before any partition is added.
    pub fn add_partitions(&mut self, id: TopicId, additional_leaders: &[i32]) -> Result<()> {
        let topic_index = self
            .topics
            .iter()
            .position(|t| t.id == id && !t.deleted)
            .ok_or(Error::UnknownTopic)?;
        self.validate_partition_growth(additional_leaders)?;
        self.topics[topic_index]
            .partitions
            .extend(additional_leaders.iter().map(|&leader| Partition {
                leader,
                epoch: 0,
                next_offset: 0,
                producers: Vec::new(),
                batches: Vec::new(),
            }));
        Ok(())
    }
    pub fn move_leader(&mut self, id: TopicId, partition: i32, broker: i32) -> Result<()> {
        if !self.brokers.iter().any(|b| b.id == broker) {
            return Err(Error::UnknownBroker);
        }
        let p = self.partition_mut(id, partition)?;
        p.epoch = p.epoch.checked_add(1).ok_or(Error::Limit("leader epoch"))?;
        p.leader = broker;
        self.stats.leader_moves += 1;
        Ok(())
    }
    pub fn leader(&self, id: TopicId, partition: i32) -> Result<(i32, i32)> {
        let topic = self
            .topics
            .iter()
            .find(|t| t.id == id && !t.deleted)
            .ok_or(Error::UnknownTopic)?;
        let p = usize::try_from(partition)
            .ok()
            .and_then(|p| topic.partitions.get(p))
            .ok_or(Error::UnknownPartition)?;
        Ok((p.leader, p.epoch))
    }
    fn partition_mut(&mut self, id: TopicId, partition: i32) -> Result<&mut Partition> {
        let topic = self
            .topics
            .iter_mut()
            .find(|t| t.id == id && !t.deleted)
            .ok_or(Error::UnknownTopic)?;
        usize::try_from(partition)
            .ok()
            .and_then(|p| topic.partitions.get_mut(p))
            .ok_or(Error::UnknownPartition)
    }
    /// Simulates broker producer-state retention loss without removing the log.
    /// Real nontransactional Kafka accepts any initial sequence when epoch state
    /// is absent. Explicit UNKNOWN_PRODUCER_ID rejection is a separate fault.
    pub fn forget_producer_state(&mut self, id: TopicId, partition: i32, pid: i64) -> Result<()> {
        self.partition_mut(id, partition)?
            .producers
            .retain(|p| p.id != pid);
        Ok(())
    }
    pub fn handle_frame(
        &mut self,
        broker: i32,
        bytes: &[u8],
        fault: FaultPlan,
    ) -> Result<BrokerAction> {
        if !self.brokers.iter().any(|b| b.id == broker) {
            return Err(Error::UnknownBroker);
        }
        if fault.throttle_time_ms < 0
            || fault
                .reject_before_commit
                .is_some_and(|c| c == code::NONE || c == code::DUPLICATE_SEQUENCE_NUMBER)
        {
            return Err(Error::InvalidConfig);
        }
        // Kafka answers an unknown ApiVersions request using response v0 so
        // newer clients can discover this model's supported range.
        if bytes.len() <= self.config.max_frame_bytes
            && let Some((_, correlation)) = unsupported_api_versions(bytes)
        {
            self.stats.requests += 1;
            if fault.drop_after_parse {
                return Ok(BrokerAction::DropRequest);
            }
            let reply = self.api_versions(0, correlation, 0, code::UNSUPPORTED_VERSION)?;
            return Ok(if fault.disconnect_before_response {
                BrokerAction::Disconnect {
                    committed_batches: 0,
                }
            } else if fault.drop_after_commit {
                BrokerAction::DropResponse {
                    committed_batches: 0,
                }
            } else {
                BrokerAction::Reply(reply)
            });
        }
        let frame = decode_request(bytes, self.decode_limits())?;
        self.stats.requests += 1;
        if fault.drop_after_parse {
            self.stats.dropped_requests += 1;
            return Ok(BrokerAction::DropRequest);
        }
        if let Some(change) = fault.leader_move {
            self.move_leader(change.topic, change.partition, change.broker)?;
        }
        let before = self.log.len();
        let response = match frame.body {
            Request::ApiVersionsRequest(_) => self.api_versions(
                frame.version,
                frame.correlation_id,
                fault.throttle_time_ms,
                code::NONE,
            )?,
            Request::MetadataRequest(wire::metadata_request::View::V12(request)) => {
                self.metadata(&request, frame.correlation_id, fault.throttle_time_ms)?
            }
            Request::InitProducerIdRequest(wire::init_producer_id_request::View::V4(request)) => {
                self.init_producer(&request, frame.correlation_id, fault.throttle_time_ms)?
            }
            Request::ProduceRequest(request) => {
                self.produce(broker, &request, frame.version, frame.correlation_id, fault)?
            }
            Request::FetchRequest(wire::fetch_request::View::V13(request)) => self.fetch(
                broker,
                &request,
                frame.correlation_id,
                fault.throttle_time_ms,
            )?,
            _ => {
                return Err(Error::InvalidRequest(
                    "SASL is outside this unauthenticated broker model",
                ));
            }
        };
        let committed_batches = self.log.len() - before;
        if fault.drop_after_commit {
            self.stats.dropped_responses += 1;
            return Ok(BrokerAction::DropResponse { committed_batches });
        }
        if fault.disconnect_before_response {
            self.stats.dropped_responses += 1;
            return Ok(BrokerAction::Disconnect { committed_batches });
        }
        Ok(BrokerAction::Reply(response))
    }
    fn decode_limits(&self) -> DecodeLimits {
        DecodeLimits {
            max_bytes: self.config.max_frame_bytes,
            max_array_elements: self
                .config
                .max_partitions
                .saturating_mul(8)
                .saturating_add(self.config.max_topics)
                .saturating_add(self.config.max_brokers),
            max_depth: 16,
            max_tags: 4096,
        }
    }
    fn encode(&self, response: Response<'_>, version: i16, correlation: i32) -> Result<Vec<u8>> {
        Ok(response
            .plan_frame(
                version,
                correlation,
                EncodeLimits {
                    max_bytes: self.config.max_frame_bytes,
                    max_metadata_bytes: self.config.max_frame_bytes,
                    max_segments: 32,
                    max_array_elements: self.decode_limits().max_array_elements,
                    max_depth: 16,
                    max_tags: 4096,
                },
            )?
            .to_vec()?)
    }
    fn api_versions(
        &self,
        version: i16,
        correlation: i32,
        throttle: i32,
        error_code: i16,
    ) -> Result<Vec<u8>> {
        let versions = [
            (0, 9, self.config.produce_max_version),
            (1, 13, 13),
            (3, 12, 12),
            (18, 0, 3),
            (22, 4, 4),
        ];
        if version == 0 {
            use wire::api_versions_response::{self as api, v0::*};
            let keys: Vec<_> = versions
                .iter()
                .map(|&(api_key, min_version, max_version)| ApiVersion {
                    api_key,
                    min_version,
                    max_version,
                    ..Default::default()
                })
                .collect();
            self.encode(
                Response::ApiVersionsResponse(api::View::V0(ApiVersionsResponse {
                    error_code,
                    api_keys: (&keys[..]).into(),
                    ..Default::default()
                })),
                version,
                correlation,
            )
        } else {
            use wire::api_versions_response::{self as api, v3::*};
            let keys: Vec<_> = versions
                .iter()
                .map(|&(api_key, min_version, max_version)| ApiVersion {
                    api_key,
                    min_version,
                    max_version,
                    ..Default::default()
                })
                .collect();
            self.encode(
                Response::ApiVersionsResponse(api::View::V3(ApiVersionsResponse {
                    error_code,
                    api_keys: (&keys[..]).into(),
                    throttle_time_ms: throttle,
                    ..Default::default()
                })),
                version,
                correlation,
            )
        }
    }
    fn init_producer(
        &mut self,
        request: &wire::init_producer_id_request::v4::InitProducerIdRequest<'_>,
        correlation: i32,
        throttle: i32,
    ) -> Result<Vec<u8>> {
        use wire::init_producer_id_response::{self as api, v4::*};
        let error = if request.transactional_id.is_some() {
            code::INVALID_REQUEST
        } else if self.issued_producers == self.config.max_producers {
            code::COORDINATOR_LOAD_IN_PROGRESS
        } else {
            code::NONE
        };
        let id = if error == 0 {
            let id = self.next_pid;
            self.next_pid = self
                .next_pid
                .checked_add(1)
                .ok_or(Error::Limit("producer IDs"))?;
            self.issued_producers += 1;
            id
        } else {
            -1
        };
        self.encode(
            Response::InitProducerIdResponse(api::View::V4(InitProducerIdResponse {
                producer_id: id,
                producer_epoch: 0,
                error_code: error,
                throttle_time_ms: throttle,
                ..Default::default()
            })),
            4,
            correlation,
        )
    }
}

/// Recognize an unsupported flexible ApiVersions probe without parsing its
/// unknown body. Length and the known request header still have to be valid.
pub fn unsupported_api_versions(bytes: &[u8]) -> Option<(i16, i32)> {
    use wire::wire::Wire;
    if bytes.len() < 15
        || i32::from_be_bytes(bytes.get(..4)?.try_into().ok()?) as usize != bytes.len() - 4
        || i16::from_be_bytes(bytes.get(4..6)?.try_into().ok()?) != 18
    {
        return None;
    }
    let version = i16::from_be_bytes(bytes.get(6..8)?.try_into().ok()?);
    if version <= 3 {
        return None;
    }
    let mut reader =
        wire::wire::Reader::new(&bytes[4..], 2, false, DecodeLimits::default()).ok()?;
    let header = wire::request_header::v2::RequestHeader::read(&mut reader).ok()?;
    Some((version, header.correlation_id))
}
