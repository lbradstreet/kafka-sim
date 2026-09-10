//! Kafka identity and metadata rows, independent of producer/consumer state.

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct TopicId(pub [u8; 16]);
impl TopicId {
    pub const ZERO: Self = Self([0; 16]);
    #[must_use]
    pub fn is_zero(self) -> bool {
        self == Self::ZERO
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(C)]
pub struct TopicPartition {
    pub topic: TopicId,
    pub partition: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionMetadata {
    pub leader: i32,
    pub leader_epoch: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataSelector<'a> {
    Name(&'a str),
    Id(TopicId),
}
