//! Immutable, credit-accounted metadata retained across refresh and retirement.
use crate::{
    control::{BrokerNode, MetadataPartition},
    credit::{Claim, CreditError, HeldCredits, Resource, SharedCredits},
    types::TopicId,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct MetadataBrokers {
    pub rows: Vec<BrokerNode>,
    _credit: HeldCredits,
}
impl MetadataBrokers {
    pub(crate) fn retain(
        rows: &[BrokerNode],
        credits: &SharedCredits,
    ) -> Result<Arc<Self>, CreditError> {
        let bytes = size_of::<Self>()
            + 2 * size_of::<usize>()
            + std::mem::size_of_val(rows)
            + rows
                .iter()
                .map(|row| row.host.len() + row.rack.as_ref().map_or(0, String::len))
                .sum::<usize>();
        let credit = credits.reserve(&[Claim {
            resource: Resource::InputBytes,
            amount: bytes,
            lane: 0,
        }])?;
        Ok(Arc::new(Self {
            rows: rows.to_vec(),
            _credit: credit,
        }))
    }
}

#[derive(Debug)]
pub struct MetadataSnapshot {
    pub id: TopicId,
    pub generation: u64,
    pub(crate) routing_generation: u32,
    pub brokers: Arc<MetadataBrokers>,
    pub partitions: Vec<MetadataPartition>,
    _credit: HeldCredits,
}
impl PartialEq for MetadataSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.generation == other.generation
            && self.brokers.rows == other.brokers.rows
            && self.partitions == other.partitions
    }
}
impl Eq for MetadataSnapshot {}
impl MetadataSnapshot {
    pub(crate) fn retain(
        id: TopicId,
        generation: u64,
        brokers: Arc<MetadataBrokers>,
        partitions: Vec<MetadataPartition>,
        credits: &SharedCredits,
    ) -> Result<Arc<Self>, CreditError> {
        let bytes = size_of::<Self>()
            + 2 * size_of::<usize>()
            + partitions.capacity() * size_of::<MetadataPartition>()
            + partitions
                .iter()
                .map(|p| {
                    (p.replicas.capacity() + p.isr.capacity() + p.offline.capacity())
                        * size_of::<i32>()
                })
                .sum::<usize>();
        let credit = credits.reserve(&[Claim {
            resource: Resource::InputBytes,
            amount: bytes,
            lane: 0,
        }])?;
        Ok(Arc::new(Self {
            id,
            generation,
            routing_generation: 0,
            brokers,
            partitions,
            _credit: credit,
        }))
    }
}
