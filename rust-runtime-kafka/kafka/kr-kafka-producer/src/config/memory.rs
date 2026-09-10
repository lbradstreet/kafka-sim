//! Checked configuration subtotals with an explicit completeness boundary.
use super::{ConfigError, ProducerConfig, add};
use crate::{client::Command, credit::Resource, lifecycle::EventEnvelope, mailbox::BoundedMailbox};
use std::fmt;

/// Configured backing for preallocated fixed-slot containers, using their real
/// element layouts on this target. Pools and ingress/event/release queues verify
/// exact capacities before publication; remaining reservations can contain
/// slack. Nested allocations and that remaining slack are explicit gaps in
/// [`MemoryBudgetReport::unaccounted`]. Construction transients are separate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixedMetadataBudget {
    /// Stable descriptor account slots and free indices for opt-in admission
    /// isolation. Live-class tree nodes remain in the OrderedIndexes gap.
    pub descriptor_admission: usize,
    /// Exact retained batch, connection and request entries/free-slot indices
    /// after successful construction.
    pub engine_object_pools: usize,
    /// Exact retained order slots, event envelopes, flush fences and terminal-
    /// batch slots after successful construction.
    pub engine_queues: usize,
    /// Exact retained backing of both command lanes and the application event
    /// ring after successful construction.
    pub client_queues: usize,
    /// Exact retained lease entries/free indices and passive input-release ring
    /// backing after successful construction.
    pub input_registry: usize,
    /// Exact retained incremental routing rows/weights and bounded actor
    /// record/choice scratch after successful construction. Callback views are
    /// temporary and are counted separately among the remaining gaps.
    pub routing_scratch: usize,
    /// Requested backing for three HDR banks, logical histogram bins and scope
    /// identities; HDR's private counter capacity is not exposed for checking.
    pub metrics: crate::telemetry::metrics::MetricsMemory,
    pub configured_bytes: usize,
}

/// Producer-owned allocations that this report does not yet bound in bytes.
/// Cardinality limits alone do not prove the allocation layout of these owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UnaccountedMemory {
    /// Extra capacity in reservations that have not been normalized or checked.
    /// Excludes pools and ingress/event/release/order/flush/terminal queues,
    /// which reject slack. Routing scratch and metrics label/distribution/index
    /// vectors also reject slack. Includes opaque HDR counter capacities.
    CollectionReservationSlack,
    /// Routing, metadata, sequence, deadline, delivery and lifecycle tree nodes,
    /// including per-topic accepted-record and resumable-settlement indexes.
    OrderedIndexes,
    /// Owned config clones, topic/broker names, partition arrays and decode trees.
    ConfigurationAndMetadata,
    /// Record/obligation vectors and per-guard credit vectors, excluding header
    /// metadata already charged to InputBytes.
    RecordAndCreditMetadata,
    /// Output-pool handles, codec-pool handles and encoder cursor/chunk vectors;
    /// compressed payload and codec workspace are already in the byte pools.
    CodecAndOutputMetadata,
    /// Actor/control work, connection futures, send-plan segment descriptors and
    /// per-plan topic-fence vectors (at most one per selected partition);
    /// staging, RX, TLS and request-arena payloads are already in the byte pools.
    ActorAndTransportMetadata,
    /// Shared/boxed owner state, per-topic write fences and Arc/Rc control blocks. The size of an Arc
    /// handle does not describe its separately allocated control block.
    SharedOwnerAllocations,
    /// Startup (including rejected reservations/partial constructors), admission,
    /// routing, authentication and response-decode transient storage whose full
    /// simultaneous peak is not yet composed into this report.
    TransientWorkingStorage,
}
const UNACCOUNTED: &[UnaccountedMemory] = &[
    UnaccountedMemory::CollectionReservationSlack,
    UnaccountedMemory::OrderedIndexes,
    UnaccountedMemory::ConfigurationAndMetadata,
    UnaccountedMemory::RecordAndCreditMetadata,
    UnaccountedMemory::CodecAndOutputMetadata,
    UnaccountedMemory::ActorAndTransportMetadata,
    UnaccountedMemory::SharedOwnerAllocations,
    UnaccountedMemory::TransientWorkingStorage,
];

/// Allocations outside the producer-core accounting contract. A host embedding
/// must add its own bounds for these components before claiming a process bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MemoryScopeExclusion {
    /// Allocator headers/rounding not exposed as collection capacity, stacks,
    /// executable pages, RSS effects, and kernel socket/ring memory.
    AllocatorAndOperatingSystem,
    /// Injected runtimes, connectors, I/O providers and TLS libraries can retain
    /// arbitrary private metadata. Their producer-credited payload buffers are
    /// still included; this exclusion covers their independent internal state.
    InjectedRuntimeAndProviderState,
    /// Host supervision, language binding registries and application allocations
    /// not transferred into a producer input lease.
    EmbeddingAndApplicationState,
}
const EXCLUSIONS: &[MemoryScopeExclusion] = &[
    MemoryScopeExclusion::AllocatorAndOperatingSystem,
    MemoryScopeExclusion::InjectedRuntimeAndProviderState,
    MemoryScopeExclusion::EmbeddingAndApplicationState,
];

/// The report is a checked subtotal, not a complete allocation-capacity bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncompleteMemoryAccounting {
    pub unaccounted: &'static [UnaccountedMemory],
}
impl fmt::Display for IncompleteMemoryAccounting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "producer memory accounting has {} unaccounted components",
            self.unaccounted.len()
        )
    }
}
impl std::error::Error for IncompleteMemoryAccounting {}

/// Configured byte pools and fixed element storage for one engine/client pair.
///
/// `configured_capacity_subtotal` is neither RSS nor a complete bound on owned
/// allocation capacity. Use [`Self::require_complete_core_bound`] to enforce
/// completeness; it currently rejects every configuration with the explicit
/// gaps below. Reservations and arithmetic are checked before resource creation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryBudgetReport {
    pub input: usize,
    pub compressed: usize,
    pub codec_workspace: usize,
    pub transport: usize,
    pub control: usize,
    pub request_metadata: usize,
    /// Sum of the six configured byte pools above, preserved independently of
    /// metadata. This field alone must not be used as a client memory bound.
    pub configured_byte_pools: usize,
    pub fixed_metadata: FixedMetadataBudget,
    /// Byte pools plus requested fixed element storage; excludes `unaccounted`.
    pub configured_capacity_subtotal: usize,
    pub unaccounted: &'static [UnaccountedMemory],
    pub exclusions: &'static [MemoryScopeExclusion],
}
impl MemoryBudgetReport {
    /// Returns a complete producer-core allocation-capacity bound when every
    /// in-scope allocation has a proved term. Scope exclusions still apply.
    ///
    /// # Errors
    /// Returns the outstanding accounting gaps. Current reports are incomplete;
    /// callers must not silently substitute the configured subtotal on error.
    pub fn require_complete_core_bound(&self) -> Result<usize, IncompleteMemoryAccounting> {
        if self.unaccounted.is_empty() {
            Ok(self.configured_capacity_subtotal)
        } else {
            Err(IncompleteMemoryAccounting {
                unaccounted: self.unaccounted,
            })
        }
    }
}

pub(super) fn fixed_metadata(
    config: &ProducerConfig,
    connections: usize,
    credits: &[usize; Resource::COUNT],
) -> Result<FixedMetadataBudget, ConfigError> {
    let field = "fixed metadata";
    let overflow = || ConfigError::Overflow { field };
    let control = credits[Resource::ControlEvents as usize];
    let events = add(
        add(
            credits[Resource::DeliveryEvents as usize],
            credits[Resource::ReleaseEvents as usize],
            field,
        )?,
        control,
        field,
    )?;
    let (engine_object_pools, engine_queues) = crate::engine::memory::configured_storage_bytes(
        config,
        connections,
        credits[Resource::RequestSlots as usize],
        control,
        events,
    )
    .ok_or_else(overflow)?;
    let client_queues = BoundedMailbox::<Command>::configured_storage_bytes(
        config.mailbox_capacity as usize,
        control,
    )
    .and_then(|commands| {
        commands.checked_add(BoundedMailbox::<EventEnvelope>::configured_storage_bytes(
            events, 0,
        )?)
    })
    .ok_or_else(overflow)?;
    let input_registry =
        crate::input::memory::configured_storage_bytes(config).ok_or_else(overflow)?;
    let routing_scratch =
        crate::actor::routing::configured_storage_bytes(config).ok_or_else(overflow)?;
    let descriptor_admission =
        crate::credit::configured_storage_bytes(config).ok_or_else(overflow)?;
    let metrics = config.metrics.memory().map_err(|error| match error {
        crate::telemetry::metrics::MetricsError::Overflow => {
            ConfigError::Overflow { field: "metrics" }
        }
        _ => ConfigError::Invalid {
            field: "metrics",
            reason: "invalid bounds or histogram storage exceeds configured limit",
        },
    })?;
    let configured_bytes = [
        descriptor_admission,
        engine_object_pools,
        engine_queues,
        client_queues,
        input_registry,
        routing_scratch,
        metrics.configured_bytes,
    ]
    .into_iter()
    .try_fold(0, |a, b| add(a, b, field))?;
    Ok(FixedMetadataBudget {
        descriptor_admission,
        engine_object_pools,
        engine_queues,
        client_queues,
        input_registry,
        routing_scratch,
        metrics,
        configured_bytes,
    })
}

pub(super) fn gaps() -> &'static [UnaccountedMemory] {
    UNACCOUNTED
}
pub(super) fn exclusions() -> &'static [MemoryScopeExclusion] {
    EXCLUSIONS
}
