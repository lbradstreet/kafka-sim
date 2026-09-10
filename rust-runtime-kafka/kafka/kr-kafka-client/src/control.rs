//! Request-independent checked Kafka control requests and owned response snapshots.
//!
//! ApiVersions returns validated advertised ranges; each caller chooses its own
//! required APIs. Metadata and SASL parsing validates complete frames and exact
//! correlation/identity sets before returning an owned result. Indexed identity
//! checks are O(n log n), retaining caller order. Scratch shares the owned-byte
//! budget and counts allocator-reported spare capacity. Each complete control
//! frame remains one synchronous encoding/parsing quantum.
use crate::{
    config::SaslMechanism,
    types::{MetadataSelector, PartitionMetadata, TopicId},
};
use kr_kafka_protocol::{
    self as wire, Request, Response, errors as code,
    frame::{ResponseFrame, decode_response},
    wire::{DecodeLimits, EncodeLimits},
};
use std::{fmt, mem::size_of, sync::Arc};

/// Checked scratch/framing support for higher-level API-specific codecs.
/// These helpers carry no connection, producer, or consumer policy.
#[doc(hidden)]
pub mod support;
use support::{Index, OwnedBudget, throttle, unique};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlLimits {
    pub frame_bytes: usize,
    pub owned_bytes: usize,
    pub api_keys: usize,
    pub topics: usize,
    pub partitions: usize,
    pub brokers: usize,
    pub string_bytes: usize,
    pub tags: usize,
    pub auth_bytes: usize,
    /// Aggregate elements across every nested decoded array.
    pub max_array_elements: usize,
}
impl Default for ControlLimits {
    fn default() -> Self {
        Self {
            frame_bytes: 1024 * 1024,
            owned_bytes: 2 * 1024 * 1024,
            api_keys: 512,
            topics: 1024,
            partitions: 65536,
            brokers: 64,
            string_bytes: 4096,
            tags: 4096,
            auth_bytes: 64 * 1024,
            max_array_elements: 65536 * (64 * 3 + 4) + 1024 + 512 + 4096,
        }
    }
}
impl ControlLimits {
    fn validate(self) -> Result<Self> {
        if self.frame_bytes < 16
            || self.frame_bytes > i32::MAX as usize
            || self.owned_bytes == 0
            || self.api_keys == 0
            || self.topics == 0
            || self.partitions == 0
            || self.brokers == 0
            || self.string_bytes == 0
            || self.tags == 0
            || self.max_array_elements == 0
            || self.auth_bytes == 0
        {
            return Err(ControlError::InvalidConfig);
        }
        Ok(self)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlError {
    InvalidConfig,
    Limit(&'static str),
    Invalid(&'static str),
    Wire(wire::wire::Error),
    Broker { api_key: i16, error_code: i16 },
    MissingCapability { api_key: i16, required: i16 },
    UnexpectedTopic,
    UnexpectedPartition,
    IdentityChanged,
    UnexpectedBody,
    UnsupportedMechanism,
}
impl From<wire::wire::Error> for ControlError {
    fn from(value: wire::wire::Error) -> Self {
        Self::Wire(value)
    }
}
impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ControlError {}
pub type Result<T> = std::result::Result<T, ControlError>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Probe {
    V3,
    V0,
}
impl Probe {
    pub fn version(self) -> i16 {
        match self {
            Self::V3 => 3,
            Self::V0 => 0,
        }
    }
}
/// One validated advertised API range. The list is sorted by API key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}
/// Broker advertisements, with no implicit requirements from any request type.
/// Clones share one immutable allocation and its optional setup reservation.
#[derive(Clone)]
pub struct Capabilities {
    probe_version: i16,
    storage: Arc<CapabilityStorage>,
}
struct CapabilityStorage {
    ranges: Vec<ApiVersionRange>,
    retained_bytes: usize,
    // Last field: release backing before returning the reservation it retained.
    guard: Option<Arc<dyn Send + Sync>>,
}
impl fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Capabilities")
            .field("probe_version", &self.probe_version)
            .field("ranges", &self.ranges())
            .finish()
    }
}
impl PartialEq for Capabilities {
    fn eq(&self, other: &Self) -> bool {
        self.probe_version == other.probe_version && self.ranges() == other.ranges()
    }
}
impl Eq for Capabilities {}
impl Capabilities {
    pub fn probe_version(&self) -> i16 {
        self.probe_version
    }
    pub fn ranges(&self) -> &[ApiVersionRange] {
        &self.storage.ranges
    }
    /// Known-backing subtotal: actual range-Vec capacity plus the shared payload.
    /// The standard library's opaque Arc control block remains unaccounted core
    /// storage; this subtotal does not establish a complete core memory bound.
    /// Allocator rounding is a separate excluded overhead. All clones share
    /// these bytes and allocate no additional range storage.
    pub fn retained_capacity_bytes(&self) -> usize {
        self.storage.retained_bytes
    }
    /// Attaches a setup reservation before the advertisement is shared. The
    /// reservation follows every later clone, even if the driver is discarded.
    /// # Errors
    /// Returns the supplied guard intact when this backing is already shared or
    /// guarded. An existing guard can never be removed or replaced through this API.
    pub fn attach_lifetime_guard(
        &mut self,
        guard: Arc<dyn Send + Sync>,
    ) -> std::result::Result<(), Arc<dyn Send + Sync>> {
        let Some(storage) = Arc::get_mut(&mut self.storage) else {
            return Err(guard);
        };
        if storage.guard.is_some() {
            return Err(guard);
        }
        storage.guard = Some(guard);
        Ok(())
    }
    pub fn supports(&self, api_key: i16, version: i16) -> bool {
        self.ranges()
            .binary_search_by_key(&api_key, |range| range.api_key)
            .is_ok_and(|index| {
                self.ranges()[index].min_version <= version
                    && version <= self.ranges()[index].max_version
            })
    }
    /// Checks one caller-supplied requirement without allocating.
    pub fn require(&self, api_key: i16, version: i16) -> Result<()> {
        if self.supports(api_key, version) {
            Ok(())
        } else {
            Err(ControlError::MissingCapability {
                api_key,
                required: version,
            })
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Negotiation {
    ProbeClassic,
    Ready(Capabilities),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrokerNode {
    pub id: i32,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataPartition {
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub offline: Vec<i32>,
    pub index: i32,
    pub error_code: i16,
    pub metadata: PartitionMetadata,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataTopic {
    /// Index into the exact selector slice used to issue this request.
    pub requested_index: usize,
    pub id: TopicId,
    pub name: Option<String>,
    pub error_code: i16,
    pub partitions: Vec<MetadataPartition>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataUpdate {
    pub throttle_ms: u32,
    pub brokers: Vec<BrokerNode>,
    pub topics: Vec<MetadataTopic>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandshakeResponse {
    pub error_code: i16,
    pub mechanisms: Vec<String>,
}
#[derive(Clone, PartialEq, Eq)]
pub struct AuthenticationResponse {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub auth_bytes: Vec<u8>,
    pub session_lifetime_ms: u64,
}
impl fmt::Debug for AuthenticationResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthenticationResponse")
            .field("error_code", &self.error_code)
            .field("auth_bytes_len", &self.auth_bytes.len())
            .field("session_lifetime_ms", &self.session_lifetime_ms)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug)]
pub struct ControlCodec {
    client_id: String,
    limits: ControlLimits,
}
impl ControlCodec {
    pub fn new(client_id: String, limits: ControlLimits) -> Result<Self> {
        let limits = limits.validate()?;
        if client_id.len() > i16::MAX as usize || client_id.len() > limits.string_bytes {
            return Err(ControlError::Limit("client ID"));
        }
        Ok(Self { client_id, limits })
    }
    pub fn limits(&self) -> ControlLimits {
        self.limits
    }
    fn decode_limits(&self) -> DecodeLimits {
        DecodeLimits {
            max_bytes: self.limits.frame_bytes,
            max_array_elements: self.limits.max_array_elements,
            max_depth: 16,
            max_tags: self.limits.tags,
        }
    }
    /// Encodes an API-specific extension request under this codec's checked frame limits.
    pub fn encode_request(
        &self,
        request: Request<'_>,
        version: i16,
        correlation: i32,
    ) -> Result<Vec<u8>> {
        Ok(request
            .plan_frame(
                version,
                correlation,
                Some(&self.client_id),
                EncodeLimits {
                    max_bytes: self.limits.frame_bytes,
                    max_metadata_bytes: self.limits.owned_bytes,
                    max_segments: 16,
                    max_array_elements: self.decode_limits().max_array_elements,
                    max_depth: 16,
                    max_tags: self.limits.tags,
                },
            )?
            .to_vec()?)
    }
    /// Decodes a complete extension response with exact API, version and correlation.
    pub fn decode_response<'a>(
        &self,
        bytes: &'a [u8],
        api_key: i16,
        version: i16,
        correlation: i32,
    ) -> Result<ResponseFrame<'a>> {
        Ok(decode_response(
            bytes,
            api_key,
            version,
            correlation,
            self.decode_limits(),
        )?)
    }
    pub fn api_versions_request(&self, correlation: i32, probe: Probe) -> Result<Vec<u8>> {
        use wire::api_versions_request::{self as api};
        let request = match probe {
            Probe::V0 => api::View::V0(Default::default()),
            Probe::V3 => api::View::V3(api::v3::ApiVersionsRequest {
                client_software_name: "kr-kafka",
                client_software_version: env!("CARGO_PKG_VERSION"),
                ..Default::default()
            }),
        };
        self.encode_request(
            Request::ApiVersionsRequest(request),
            probe.version(),
            correlation,
        )
    }
    /// Only a fully validated UNSUPPORTED_VERSION response permits the classic
    /// probe. A malformed v3 response is never silently interpreted as success.
    pub fn parse_api_versions(
        &self,
        bytes: &[u8],
        correlation: i32,
        probe: Probe,
    ) -> Result<Negotiation> {
        let response = match self.decode_response(bytes, 18, probe.version(), correlation) {
            Ok(response) => response,
            Err(original) if probe == Probe::V3 => {
                match self.parse_api_versions(bytes, correlation, Probe::V0) {
                    Err(ControlError::Broker {
                        api_key: 18,
                        error_code: code::UNSUPPORTED_VERSION,
                    }) => return Ok(Negotiation::ProbeClassic),
                    _ => return Err(original),
                }
            }
            Err(error) => return Err(error),
        };
        let mut budget = OwnedBudget::new(self.limits);
        let (error_code, mut ranges) = match response.body {
            Response::ApiVersionsResponse(wire::api_versions_response::View::V0(response)) => {
                let mut ranges = budget.vec::<ApiVersionRange>(
                    response.api_keys.len(),
                    self.limits.api_keys,
                    "API keys",
                )?;
                for range in response.api_keys.iter() {
                    let range = range?;
                    ranges.push(ApiVersionRange {
                        api_key: range.api_key,
                        min_version: range.min_version,
                        max_version: range.max_version,
                    });
                }
                (response.error_code, ranges)
            }
            Response::ApiVersionsResponse(wire::api_versions_response::View::V3(response)) => {
                throttle(response.throttle_time_ms)?;
                let mut ranges = budget.vec::<ApiVersionRange>(
                    response.api_keys.len(),
                    self.limits.api_keys,
                    "API keys",
                )?;
                for range in response.api_keys.iter() {
                    let range = range?;
                    ranges.push(ApiVersionRange {
                        api_key: range.api_key,
                        min_version: range.min_version,
                        max_version: range.max_version,
                    });
                }
                (response.error_code, ranges)
            }
            _ => return Err(ControlError::UnexpectedBody),
        };
        ranges.sort_unstable_by_key(|range| range.api_key);
        for (index, range) in ranges.iter().enumerate() {
            if range.api_key < 0
                || range.min_version < 0
                || range.max_version < range.min_version
                || index != 0 && ranges[index - 1].api_key == range.api_key
            {
                return Err(ControlError::Invalid("API ranges"));
            }
        }
        if error_code == code::UNSUPPORTED_VERSION && probe == Probe::V3 {
            return Ok(Negotiation::ProbeClassic);
        }
        if error_code != 0 {
            return Err(ControlError::Broker {
                api_key: 18,
                error_code,
            });
        }
        budget.charge(size_of::<CapabilityStorage>())?;
        let retained_bytes = ranges
            .capacity()
            .checked_mul(size_of::<ApiVersionRange>())
            .and_then(|bytes| bytes.checked_add(size_of::<CapabilityStorage>()))
            .ok_or(ControlError::Limit("owned bytes"))?;
        Ok(Negotiation::Ready(Capabilities {
            probe_version: probe.version(),
            storage: Arc::new(CapabilityStorage {
                ranges,
                retained_bytes,
                guard: None,
            }),
        }))
    }
    /// Supply selectors from the caller's metadata binding: names are used only while a
    /// handle is resolving, then every refresh uses its immutable bound ID.
    pub fn metadata_request(
        &self,
        correlation: i32,
        selectors: &[MetadataSelector<'_>],
    ) -> Result<Vec<u8>> {
        let mut budget = OwnedBudget::new(self.limits);
        // Validation does not need this scratch during descriptor construction
        // or wire encoding; do not extend its lifetime into those phases.
        drop(selector_index(selectors, &mut budget)?);
        use wire::metadata_request::{self as api, v12::*};
        budget.array::<MetadataRequestTopic<'_>>(
            selectors.len(),
            self.limits.topics,
            "metadata selectors",
        )?;
        let topics: Vec<_> = selectors
            .iter()
            .map(|selector| match selector {
                MetadataSelector::Name(name) => MetadataRequestTopic {
                    name: Some(name),
                    ..Default::default()
                },
                MetadataSelector::Id(id) => MetadataRequestTopic {
                    topic_id: id.0,
                    name: None,
                    ..Default::default()
                },
            })
            .collect();
        self.encode_request(
            Request::MetadataRequest(api::View::V12(MetadataRequest {
                topics: Some((&topics[..]).into()),
                allow_auto_topic_creation: false,
                include_topic_authorized_operations: false,
                ..Default::default()
            })),
            12,
            correlation,
        )
    }
    pub fn parse_metadata(
        &self,
        bytes: &[u8],
        correlation: i32,
        expected: &[MetadataSelector<'_>],
    ) -> Result<MetadataUpdate> {
        let mut budget = OwnedBudget::new(self.limits);
        let mut expected_index = selector_index(expected, &mut budget)?;
        let Response::MetadataResponse(wire::metadata_response::View::V12(response)) =
            self.decode_response(bytes, 3, 12, correlation)?.body
        else {
            return Err(ControlError::UnexpectedBody);
        };
        let throttle_ms = throttle(response.throttle_time_ms)?;
        let mut brokers =
            budget.vec::<BrokerNode>(response.brokers.len(), self.limits.brokers, "brokers")?;
        let mut broker_ids =
            budget.vec(response.brokers.len(), self.limits.brokers, "broker IDs")?;
        for broker in response.brokers.iter() {
            let broker = broker?;
            let node = budget.node(broker.node_id, broker.host, broker.port, broker.rack)?;
            broker_ids.push(node.id);
            brokers.push(node);
        }
        unique(
            &mut broker_ids,
            ControlError::Invalid("duplicate broker ID"),
        )?;
        if response.controller_id < -1
            || response.controller_id >= 0
                && broker_ids.binary_search(&response.controller_id).is_err()
        {
            return Err(ControlError::Invalid("controller ID"));
        }
        if response.topics.len() != expected.len() {
            return Err(ControlError::UnexpectedTopic);
        }
        budget.array::<MetadataTopic>(response.topics.len(), self.limits.topics, "topics")?;
        let mut topics = Vec::with_capacity(response.topics.len());
        let mut topic_ids = budget.vec(response.topics.len(), self.limits.topics, "topic IDs")?;
        // One allocation for the actual largest validated replica list. Reading
        // borrowed views twice is linear in frame size and never materializes a tree.
        let mut replica_max = 0;
        let mut partition_total = 0usize;
        for topic in response.topics.iter() {
            let topic = topic?;
            partition_total = partition_total
                .checked_add(topic.partitions.len())
                .ok_or(ControlError::Limit("partitions"))?;
            if partition_total > self.limits.partitions {
                return Err(ControlError::Limit("partitions"));
            }
            for partition in topic.partitions.iter() {
                let partition = partition?;
                for nodes in [
                    &partition.replica_nodes,
                    &partition.isr_nodes,
                    &partition.offline_replicas,
                ] {
                    if nodes.len() > self.limits.brokers {
                        return Err(ControlError::Limit("replicas"));
                    }
                    replica_max = replica_max.max(nodes.len());
                }
            }
        }
        let mut replicas = budget.vec(replica_max, self.limits.brokers, "replicas")?;
        let mut total_partitions = 0usize;
        for topic in response.topics.iter() {
            let topic = topic?;
            let id = TopicId(topic.topic_id);
            let by_id = expected_index.get(&SelectorKey::Id(id));
            let by_name = topic
                .name
                .and_then(|name| expected_index.get(&SelectorKey::Name(name)));
            // Preserve the old first-matching-selector rule for mixed name/ID
            // expectations. Ambiguous repeated identities still fail below.
            let (slot, requested_index) = by_id
                .into_iter()
                .chain(by_name)
                .min_by_key(|(_, index)| *index)
                .ok_or(ControlError::IdentityChanged)?;
            if !expected_index.mark(slot) {
                return Err(ControlError::UnexpectedTopic);
            }
            if !id.is_zero() {
                topic_ids.push(id);
            }
            if topic.error_code == 0
                && (id.is_zero() || topic.name.is_none() || topic.partitions.is_empty())
            {
                return Err(ControlError::Invalid(
                    "successful topic identity or partition count",
                ));
            }
            total_partitions = total_partitions
                .checked_add(topic.partitions.len())
                .ok_or(ControlError::Limit("partitions"))?;
            if total_partitions > self.limits.partitions {
                return Err(ControlError::Limit("partitions"));
            }
            budget.array::<MetadataPartition>(
                topic.partitions.len(),
                self.limits.partitions,
                "partitions",
            )?;
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions.iter() {
                let p = partition?;
                if p.partition_index < 0 || p.leader_id < -1 || p.leader_epoch < -1 {
                    return Err(ControlError::Invalid("partition metadata"));
                }
                for nodes in [&p.replica_nodes, &p.isr_nodes, &p.offline_replicas] {
                    if nodes.len() > self.limits.brokers {
                        return Err(ControlError::Limit("replicas"));
                    }
                    replicas.clear();
                    for node in nodes.iter() {
                        let node = node?;
                        if node < 0 {
                            return Err(ControlError::Invalid("replica node"));
                        }
                        replicas.push(node);
                    }
                    unique(&mut replicas, ControlError::Invalid("replica node"))?;
                }
                let mut retained = [Vec::new(), Vec::new(), Vec::new()];
                for (owned, nodes) in
                    retained
                        .iter_mut()
                        .zip([&p.replica_nodes, &p.isr_nodes, &p.offline_replicas])
                {
                    *owned = budget.vec(nodes.len(), self.limits.brokers, "replica retention")?;
                    for node in nodes.iter() {
                        owned.push(node?);
                    }
                }
                let [replicas, isr, offline] = retained;
                partitions.push(MetadataPartition {
                    replicas,
                    isr,
                    offline,
                    index: p.partition_index,
                    error_code: p.error_code,
                    metadata: PartitionMetadata {
                        leader: p.leader_id,
                        leader_epoch: p.leader_epoch,
                    },
                });
            }
            partitions.sort_unstable_by_key(|p| p.index);
            if partitions
                .iter()
                .enumerate()
                .any(|(index, p)| p.index as usize != index)
            {
                return Err(ControlError::Invalid("noncontiguous metadata partitions"));
            }
            topics.push(MetadataTopic {
                requested_index,
                id,
                name: budget.optional_string(topic.name)?,
                error_code: topic.error_code,
                partitions,
            });
        }
        unique(&mut topic_ids, ControlError::UnexpectedTopic)?;
        topics.sort_unstable_by_key(|t| t.requested_index);
        Ok(MetadataUpdate {
            throttle_ms,
            brokers,
            topics,
            cluster_id: budget.optional_string(response.cluster_id)?,
            controller_id: response.controller_id,
        })
    }
    pub fn sasl_handshake_request(
        &self,
        correlation: i32,
        mechanism: SaslMechanism,
    ) -> Result<Vec<u8>> {
        use wire::sasl_handshake_request::{self as api, v1::*};
        self.encode_request(
            Request::SaslHandshakeRequest(api::View::V1(SaslHandshakeRequest {
                mechanism: mechanism_name(mechanism),
                ..Default::default()
            })),
            1,
            correlation,
        )
    }
    pub fn parse_sasl_handshake(
        &self,
        bytes: &[u8],
        correlation: i32,
        required: SaslMechanism,
    ) -> Result<HandshakeResponse> {
        self.parse_sasl_handshake_with_owned_limit(
            bytes,
            correlation,
            required,
            self.limits.owned_bytes,
        )
    }
    /// Parses under a reduced owned allowance while another control response
    /// remains retained. The caller subtracts those known retained bytes first.
    /// No codec, client ID, clock or request policy is cloned or changed.
    pub fn parse_sasl_handshake_with_owned_limit(
        &self,
        bytes: &[u8],
        correlation: i32,
        required: SaslMechanism,
        owned_bytes: usize,
    ) -> Result<HandshakeResponse> {
        let mut budget = self.response_budget(owned_bytes)?;
        let Response::SaslHandshakeResponse(wire::sasl_handshake_response::View::V1(response)) =
            self.decode_response(bytes, 17, 1, correlation)?.body
        else {
            return Err(ControlError::UnexpectedBody);
        };
        budget.array::<String>(
            response.mechanisms.len(),
            self.limits.api_keys,
            "mechanisms",
        )?;
        let mut mechanisms = Vec::with_capacity(response.mechanisms.len());
        for mechanism in response.mechanisms.iter() {
            let mechanism = budget.string(mechanism?)?;
            mechanisms.push(mechanism);
        }
        let mut names = budget.vec(mechanisms.len(), self.limits.api_keys, "mechanism names")?;
        names.extend(mechanisms.iter().map(String::as_str));
        unique(
            &mut names,
            ControlError::Invalid("duplicate SASL mechanism"),
        )?;
        if response.error_code == 0 && names.binary_search(&mechanism_name(required)).is_err() {
            return Err(ControlError::UnsupportedMechanism);
        }
        Ok(HandshakeResponse {
            error_code: response.error_code,
            mechanisms,
        })
    }
    pub fn sasl_authenticate_request(
        &self,
        correlation: i32,
        auth_bytes: &[u8],
    ) -> Result<Vec<u8>> {
        if auth_bytes.len() > self.limits.auth_bytes {
            return Err(ControlError::Limit("auth bytes"));
        }
        use wire::sasl_authenticate_request::{self as api, v2::*};
        self.encode_request(
            Request::SaslAuthenticateRequest(api::View::V2(SaslAuthenticateRequest {
                auth_bytes,
                ..Default::default()
            })),
            2,
            correlation,
        )
    }
    pub fn parse_sasl_authenticate(
        &self,
        bytes: &[u8],
        correlation: i32,
    ) -> Result<AuthenticationResponse> {
        self.parse_sasl_authenticate_with_owned_limit(bytes, correlation, self.limits.owned_bytes)
    }
    /// Parses an authentication response under a remaining shared-work allowance.
    /// The override may reduce, but never increase, this codec's owned-byte limit.
    pub fn parse_sasl_authenticate_with_owned_limit(
        &self,
        bytes: &[u8],
        correlation: i32,
        owned_bytes: usize,
    ) -> Result<AuthenticationResponse> {
        let mut budget = self.response_budget(owned_bytes)?;
        let Response::SaslAuthenticateResponse(wire::sasl_authenticate_response::View::V2(
            response,
        )) = self.decode_response(bytes, 36, 2, correlation)?.body
        else {
            return Err(ControlError::UnexpectedBody);
        };
        if response.session_lifetime_ms < 0 {
            return Err(ControlError::Invalid("SASL session lifetime"));
        }
        budget.array::<u8>(
            response.auth_bytes.len(),
            self.limits.auth_bytes,
            "auth bytes",
        )?;
        Ok(AuthenticationResponse {
            error_code: response.error_code,
            error_message: budget.optional_string(response.error_message)?,
            auth_bytes: response.auth_bytes.to_vec(),
            session_lifetime_ms: response.session_lifetime_ms as u64,
        })
    }
    fn response_budget(&self, owned_bytes: usize) -> Result<OwnedBudget> {
        if owned_bytes > self.limits.owned_bytes {
            return Err(ControlError::InvalidConfig);
        }
        Ok(OwnedBudget::new(ControlLimits {
            owned_bytes,
            ..self.limits
        }))
    }
}
pub fn mechanism_name(mechanism: SaslMechanism) -> &'static str {
    match mechanism {
        SaslMechanism::Plain => "PLAIN",
        SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
        SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
    }
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SelectorKey<'a> {
    Name(&'a str),
    Id(TopicId),
}
fn selector_index<'a>(
    selectors: &[MetadataSelector<'a>],
    budget: &mut OwnedBudget,
) -> Result<Index<SelectorKey<'a>>> {
    if selectors.len() > budget.limits.topics {
        return Err(ControlError::Limit("metadata selectors"));
    }
    for selector in selectors {
        match selector {
            MetadataSelector::Name(name)
                if name.is_empty() || name.len() > budget.limits.string_bytes =>
            {
                return Err(ControlError::Invalid("topic name"));
            }
            MetadataSelector::Id(id) if id.is_zero() => {
                return Err(ControlError::Invalid("topic ID"));
            }
            _ => {}
        }
    }
    Index::new(
        selectors.iter().map(|selector| match selector {
            MetadataSelector::Name(name) => SelectorKey::Name(name),
            MetadataSelector::Id(id) => SelectorKey::Id(*id),
        }),
        budget,
        budget.limits.topics,
        "metadata selectors",
        ControlError::Invalid("duplicate metadata selector"),
    )
}

#[cfg(test)]
mod indexed_tests;
#[cfg(test)]
mod tests;
