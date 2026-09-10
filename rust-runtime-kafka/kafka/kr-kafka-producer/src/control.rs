//! Producer-specific control policy layered over the reusable Kafka client codec.
//! Produce13/InitProducerId4 validation and producer identity interpretation remain
//! here; generic ApiVersions, Metadata and SASL use the shared checked codec.
use crate::{
    config::{ProducerConfig, SecurityConfig},
    topic::PartitionMetadata,
    types::{ProducerIdentity, TopicId, TopicPartition},
};
pub use common::{
    AuthenticationResponse, BrokerNode, ControlError, HandshakeResponse, MetadataPartition,
    MetadataTopic, MetadataUpdate, Probe, Result, mechanism_name,
};
use kr_kafka_client::control::{
    self as common,
    support::{Index, OwnedBudget, throttle, unique},
};
use kr_kafka_protocol::{self as wire, Request, Response};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlLimits {
    pub frame_bytes: usize,
    pub owned_bytes: usize,
    pub api_keys: usize,
    pub topics: usize,
    pub partitions: usize,
    pub produce_partitions: usize,
    pub brokers: usize,
    pub string_bytes: usize,
    pub tags: usize,
    pub record_errors: usize,
    pub auth_bytes: usize,
}
impl Default for ControlLimits {
    fn default() -> Self {
        Self {
            frame_bytes: 1024 * 1024,
            owned_bytes: 2 * 1024 * 1024,
            api_keys: 512,
            topics: 1024,
            partitions: 65536,
            produce_partitions: 1024,
            brokers: 64,
            string_bytes: 4096,
            tags: 4096,
            record_errors: 4096,
            auth_bytes: 64 * 1024,
        }
    }
}
impl ControlLimits {
    pub fn from_config(config: &ProducerConfig) -> Self {
        Self {
            frame_bytes: config.rx_bytes_per_connection as usize,
            owned_bytes: config.control_reserve_bytes,
            topics: config.max_open_topics as usize,
            partitions: config.max_batches as usize,
            produce_partitions: config.request_max_partitions as usize,
            brokers: config.brokers_max as usize,
            string_bytes: Self::default().string_bytes.max(config.client_id.len()),
            auth_bytes: config.control_reserve_bytes.min(64 * 1024),
            ..Default::default()
        }
    }
    fn validate(self) -> Result<Self> {
        if self.frame_bytes < 16
            || self.frame_bytes > i32::MAX as usize
            || self.owned_bytes == 0
            || self.api_keys == 0
            || self.topics == 0
            || self.partitions == 0
            || self.produce_partitions == 0
            || self.brokers == 0
            || self.string_bytes == 0
            || self.tags == 0
            || self.record_errors == 0
            || self.auth_bytes == 0
        {
            return Err(ControlError::InvalidConfig);
        }
        Ok(self)
    }
}
impl ControlLimits {
    /// Shared framing limits, including all producer extension array elements.
    pub fn common(self) -> common::ControlLimits {
        common::ControlLimits {
            frame_bytes: self.frame_bytes,
            owned_bytes: self.owned_bytes,
            api_keys: self.api_keys,
            topics: self.topics,
            partitions: self.partitions,
            brokers: self.brokers,
            string_bytes: self.string_bytes,
            tags: self.tags,
            auth_bytes: self.auth_bytes,
            max_array_elements: self
                .partitions
                .saturating_mul(self.brokers.saturating_mul(3).saturating_add(4))
                .saturating_add(self.topics)
                .saturating_add(self.api_keys)
                .saturating_add(self.record_errors),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capabilities {
    pub api_versions: i16,
    pub produce: i16,
    pub metadata: i16,
    pub init_producer_id: i16,
    pub sasl_handshake: Option<i16>,
    pub sasl_authenticate: Option<i16>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Negotiation {
    ProbeClassic,
    Ready(Capabilities),
}
impl Capabilities {
    /// Validates the producer profile at the generic connector boundary.
    pub fn from_advertised(advertised: &common::Capabilities, require_sasl: bool) -> Result<Self> {
        let required = [(18, 3), (0, 13), (3, 12), (22, 4), (17, 1), (36, 2)];
        for &(api_key, version) in required.iter().take(if require_sasl { 6 } else { 4 }) {
            advertised.require(api_key, version)?;
        }
        Ok(Self {
            api_versions: 3,
            produce: 13,
            metadata: 12,
            init_producer_id: 4,
            sasl_handshake: require_sasl.then_some(1),
            sasl_authenticate: require_sasl.then_some(2),
        })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdentityResponse {
    pub throttle_ms: u32,
    pub error_code: i16,
    pub identity: Option<ProducerIdentity>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordError {
    pub batch_index: i32,
    pub message: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducePartition {
    pub partition: TopicPartition,
    pub error_code: i16,
    pub base_offset: Option<i64>,
    pub timestamp: Option<i64>,
    pub log_start_offset: Option<i64>,
    pub current_leader: Option<PartitionMetadata>,
    pub record_errors: Vec<RecordError>,
    pub error_message: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProduceUpdate {
    pub throttle_ms: u32,
    pub partitions: Vec<ProducePartition>,
    pub brokers: Vec<BrokerNode>,
}
#[derive(Clone, Debug)]
pub struct ControlCodec {
    common: common::ControlCodec,
    require_sasl: bool,
    limits: ControlLimits,
}
impl std::ops::Deref for ControlCodec {
    type Target = common::ControlCodec;
    fn deref(&self) -> &Self::Target {
        &self.common
    }
}
impl ControlCodec {
    pub fn new(client_id: String, require_sasl: bool, limits: ControlLimits) -> Result<Self> {
        let limits = limits.validate()?;
        Ok(Self {
            common: common::ControlCodec::new(client_id, limits.common())?,
            require_sasl,
            limits,
        })
    }
    pub fn from_config(config: &ProducerConfig) -> Result<Self> {
        Self::new(
            config.client_id.clone(),
            matches!(config.security, SecurityConfig::SaslTls { .. }),
            ControlLimits::from_config(config),
        )
    }
    pub fn limits(&self) -> ControlLimits {
        self.limits
    }
    pub fn shared(&self) -> &common::ControlCodec {
        &self.common
    }
    pub fn into_shared(self) -> common::ControlCodec {
        self.common
    }
    pub fn parse_api_versions(
        &self,
        bytes: &[u8],
        correlation: i32,
        probe: Probe,
    ) -> Result<Negotiation> {
        match self.common.parse_api_versions(bytes, correlation, probe)? {
            common::Negotiation::ProbeClassic => Ok(Negotiation::ProbeClassic),
            common::Negotiation::Ready(advertised) => Ok(Negotiation::Ready(
                Capabilities::from_advertised(&advertised, self.require_sasl)?,
            )),
        }
    }
    pub fn init_producer_id_request(
        &self,
        correlation: i32,
        previous: Option<ProducerIdentity>,
    ) -> Result<Vec<u8>> {
        if previous.is_some_and(|id| !id.is_valid()) {
            return Err(ControlError::Invalid("producer identity"));
        }
        use wire::init_producer_id_request::{self as api, v4::*};
        self.common.encode_request(
            Request::InitProducerIdRequest(api::View::V4(InitProducerIdRequest {
                transactional_id: None,
                transaction_timeout_ms: 0,
                producer_id: previous.map_or(-1, |i| i.producer_id),
                producer_epoch: previous.map_or(-1, |i| i.epoch),
                ..Default::default()
            })),
            4,
            correlation,
        )
    }
    pub fn parse_identity(&self, bytes: &[u8], correlation: i32) -> Result<IdentityResponse> {
        let Response::InitProducerIdResponse(wire::init_producer_id_response::View::V4(response)) =
            self.common.decode_response(bytes, 22, 4, correlation)?.body
        else {
            return Err(ControlError::UnexpectedBody);
        };
        let identity = ProducerIdentity {
            producer_id: response.producer_id,
            epoch: response.producer_epoch,
        };
        if response.error_code == 0 && !identity.is_valid() {
            return Err(ControlError::Invalid("producer identity"));
        }
        Ok(IdentityResponse {
            throttle_ms: throttle(response.throttle_time_ms)?,
            error_code: response.error_code,
            identity: (response.error_code == 0).then_some(identity),
        })
    }
    pub fn parse_produce13(
        &self,
        bytes: &[u8],
        correlation: i32,
        expected: &[TopicPartition],
    ) -> Result<ProduceUpdate> {
        if expected.len() > self.limits.produce_partitions {
            return Err(ControlError::Limit("expected partitions"));
        }
        for p in expected {
            if p.topic.is_zero() || p.partition < 0 {
                return Err(ControlError::Invalid("expected partition set"));
            }
        }
        let mut budget = OwnedBudget::new(self.limits.common());
        let mut expected_index = Index::new(
            expected.iter().copied(),
            &mut budget,
            self.limits.produce_partitions,
            "expected partitions",
            ControlError::Invalid("expected partition set"),
        )?;
        let Response::ProduceResponse(wire::produce_response::View::V13(response)) =
            self.common.decode_response(bytes, 0, 13, correlation)?.body
        else {
            return Err(ControlError::UnexpectedBody);
        };
        let throttle_ms = throttle(response.throttle_time_ms)?;
        let mut partitions = budget.vec::<ProducePartition>(
            expected.len(),
            self.limits.produce_partitions,
            "partitions",
        )?;
        if response.responses.len() > self.limits.topics {
            return Err(ControlError::Limit("topics"));
        }
        let mut topic_ids =
            budget.vec(response.responses.len(), self.limits.topics, "topic IDs")?;
        let mut error_max = 0;
        for topic in response.responses.iter() {
            let topic = topic?;
            topic_ids.push(TopicId(topic.topic_id));
            for partition in topic.partition_responses.iter() {
                let partition = partition?;
                if partition.record_errors.len() > self.limits.record_errors {
                    return Err(ControlError::Limit("record errors"));
                }
                error_max = error_max.max(partition.record_errors.len());
            }
        }
        unique(&mut topic_ids, ControlError::UnexpectedTopic)?;
        let mut error_ids =
            budget.vec(error_max, self.limits.record_errors, "record error indices")?;
        let mut total_errors = 0usize;
        for topic in response.responses.iter() {
            let topic = topic?;
            let id = TopicId(topic.topic_id);
            if id.is_zero()
                || topic.partition_responses.is_empty()
                || expected_index
                    .lower_bound(&TopicPartition {
                        topic: id,
                        partition: 0,
                    })
                    .is_none_or(|partition| partition.topic != id)
            {
                return Err(ControlError::UnexpectedTopic);
            }
            for p in topic.partition_responses.iter() {
                let p = p?;
                let partition = TopicPartition {
                    topic: id,
                    partition: p.index,
                };
                let (slot, _) = expected_index
                    .get(&partition)
                    .ok_or(ControlError::UnexpectedPartition)?;
                if !expected_index.mark(slot) {
                    return Err(ControlError::UnexpectedPartition);
                }
                if (p.error_code == 0 && p.base_offset < 0)
                    || p.base_offset < -1
                    || p.log_append_time_ms < -1
                    || p.log_start_offset < -1
                {
                    return Err(ControlError::Invalid("successful Produce offset"));
                }
                let leader_id = p.current_leader.leader_id;
                let leader_epoch = p.current_leader.leader_epoch;
                if leader_id < -1 || leader_epoch < -1 {
                    return Err(ControlError::Invalid("current leader hint"));
                }
                // Kafka can retain an epoch while its leader ID is unknown
                // during startup. Like Java Sender's KIP-951 handling, use a
                // hint only when both independent sentinel fields are known;
                // the response error still drives the normal metadata refresh.
                let leader = (leader_id >= 0 && leader_epoch >= 0).then_some(PartitionMetadata {
                    leader: leader_id,
                    leader_epoch,
                });
                total_errors = total_errors
                    .checked_add(p.record_errors.len())
                    .ok_or(ControlError::Limit("record errors"))?;
                if total_errors > self.limits.record_errors {
                    return Err(ControlError::Limit("record errors"));
                }
                let mut errors = budget.vec(
                    p.record_errors.len(),
                    self.limits.record_errors,
                    "record errors",
                )?;
                error_ids.clear();
                for error in p.record_errors.iter() {
                    let error = error?;
                    if error.batch_index < 0 {
                        return Err(ControlError::Invalid("record error index"));
                    }
                    error_ids.push(error.batch_index);
                    errors.push(RecordError {
                        batch_index: error.batch_index,
                        message: budget.optional_string(error.batch_index_error_message)?,
                    });
                }
                unique(&mut error_ids, ControlError::Invalid("record error index"))?;
                partitions.push(ProducePartition {
                    partition,
                    error_code: p.error_code,
                    base_offset: (p.error_code == 0).then_some(p.base_offset),
                    timestamp: (p.log_append_time_ms >= 0).then_some(p.log_append_time_ms),
                    log_start_offset: (p.log_start_offset >= 0).then_some(p.log_start_offset),
                    current_leader: leader,
                    record_errors: errors,
                    error_message: budget.optional_string(p.error_message)?,
                });
            }
        }
        if partitions.len() != expected.len() {
            return Err(ControlError::UnexpectedPartition);
        }
        partitions.sort_unstable_by_key(|partition| partition.partition);
        expected_index.restore_order(&mut partitions);
        let mut brokers = budget.vec(
            response.node_endpoints.len(),
            self.limits.brokers,
            "brokers",
        )?;
        let mut broker_ids = budget.vec(
            response.node_endpoints.len(),
            self.limits.brokers,
            "broker IDs",
        )?;
        for node in response.node_endpoints.iter() {
            let node = node?;
            let node = budget.node(node.node_id, node.host, node.port, node.rack)?;
            broker_ids.push(node.id);
            brokers.push(node);
        }
        unique(
            &mut broker_ids,
            ControlError::Invalid("duplicate broker ID"),
        )?;
        Ok(ProduceUpdate {
            throttle_ms,
            partitions,
            brokers,
        })
    }
}

#[cfg(test)]
mod tests;
