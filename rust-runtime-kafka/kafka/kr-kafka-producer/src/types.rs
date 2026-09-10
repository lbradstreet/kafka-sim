//! Explicit record, topic, delivery, and work identities shared by the engine.
use kr_runtime::RuntimeDuration;

macro_rules! id {
    ($name:ident,$base:ty) => {
        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
        #[repr(transparent)]
        pub struct $name(pub $base);
    };
}
id!(RecordToken, u64);
id!(LeaseId, u64);
id!(FlushToken, u64);
id!(TopicHandle, u32);

pub use kr_kafka_client::types::{TopicId, TopicPartition};

/// Kafka sequences wrap at i32::MAX, independently of slot generations/tokens.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct Sequence(i32);
impl Sequence {
    pub const ZERO: Self = Self(0);
    #[must_use]
    pub const fn new(value: i32) -> Option<Self> {
        if value >= 0 { Some(Self(value)) } else { None }
    }
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
    #[must_use]
    pub const fn advance(self, count: u32) -> Self {
        Self(((self.0 as u64 + count as u64) & 0x7fff_ffff) as i32)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct ProducerIdentity {
    pub producer_id: i64,
    pub epoch: i16,
}
impl ProducerIdentity {
    /// Next nontransactional generation, or None when invalid/exhausted. Epochs
    /// never wrap: exhaustion requires a fresh broker-assigned producer ID.
    #[must_use]
    pub const fn next_epoch(self) -> Option<Self> {
        if !self.is_valid() {
            return None;
        }
        match self.epoch.checked_add(1) {
            Some(epoch) => Some(Self { epoch, ..self }),
            None => None,
        }
    }
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.producer_id >= 0 && self.epoch >= 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum FailureReason {
    None = 0,
    Deadline = 1,
    Cancelled = 2,
    TopicDeleted = 3,
    TopicResolution = 4,
    PartitionFailed = 5,
    /// Encoded record/batch exceeds a client or broker size limit, with any codec.
    /// The historical variant name and ABI discriminant remain stable.
    CompressedTooLarge = 6,
    InvalidRecord = 7,
    BrokerRejected = 8,
    ProducerFenced = 9,
    ProtocolViolation = 10,
    Transport = 11,
    RuntimeFailed = 12,
    SequenceUnresolved = 13,
    Closed = 14,
    ResourceExhausted = 15,
    Authentication = 16,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum DeliveryKind {
    Acked = 0,
    NotWritten = 1,
    Unknown = 2,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct DeliveryOutcome {
    pub kind: DeliveryKind,
    pub reason: FailureReason,
}
impl DeliveryOutcome {
    pub const ACKED: Self = Self {
        kind: DeliveryKind::Acked,
        reason: FailureReason::None,
    };
    #[must_use]
    pub const fn not_written(reason: FailureReason) -> Self {
        Self {
            kind: DeliveryKind::NotWritten,
            reason,
        }
    }
    #[must_use]
    pub const fn unknown(reason: FailureReason) -> Self {
        Self {
            kind: DeliveryKind::Unknown,
            reason,
        }
    }
    #[must_use]
    pub const fn unresolved(transmitted: bool, reason: FailureReason) -> Self {
        if transmitted {
            Self::unknown(reason)
        } else {
            Self::not_written(reason)
        }
    }
}
/// Fixed-width optional integer for the C ABI; never relies on Option layout.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct OptionalI64 {
    pub value: i64,
    pub present: u32,
}
impl From<Option<i64>> for OptionalI64 {
    fn from(value: Option<i64>) -> Self {
        match value {
            Some(value) => Self { value, present: 1 },
            None => Self::default(),
        }
    }
}
impl OptionalI64 {
    #[must_use]
    pub const fn get(self) -> Option<i64> {
        if self.present == 1 {
            Some(self.value)
        } else {
            None
        }
    }
}
/// Terminal events publish in native admission order within each routed
/// partition, including failures and deadlines. Different partitions have no
/// ordering guarantee. A record with unresolved routing may temporarily delay
/// later delivery events for its topic until its partition or failure is known.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct DeliveryEvent {
    pub token: RecordToken,
    pub user_token: u64,
    pub topic: TopicHandle,
    pub partition: TopicPartition,
    pub outcome: DeliveryOutcome,
    /// Absolute offset of this record, when the broker acknowledged it.
    pub base_offset: OptionalI64,
    /// The optional broker ProduceResponse `log_append_time_ms` for the batch.
    /// This is not necessarily this record's encoded CreateTime: a duplicate
    /// retry response may contain the original batch's maximum timestamp.
    pub timestamp: OptionalI64,
    pub attempts: u32,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C, u32)]
pub enum Event {
    Delivery(DeliveryEvent),
    InputReleased {
        lease: LeaseId,
    },
    FlushDone {
        token: FlushToken,
    },
    TopicReady {
        topic: TopicHandle,
        id: TopicId,
        partitions: i32,
    },
    TopicFailed {
        topic: TopicHandle,
        code: u32,
    },
    Closed {
        /// Lifetime Unknown deliveries, saturated at u32::MAX for the existing
        /// event/FFI layout. EngineStatus::unknown retains the exact u64 count.
        unresolved: u32,
    },
    Fatal {
        code: u32,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkBudget {
    pub bytes: u32,
    pub items: u32,
}
impl Default for WorkBudget {
    fn default() -> Self {
        Self {
            bytes: 64 * 1024,
            items: 128,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Progress {
    pub bytes: u32,
    pub items: u32,
    pub remaining_immediate: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SealReason {
    Target,
    Linger,
    Flush,
    HardLimit,
    Deadline,
    ContextReclaimed,
    Sparse,
    RequestGather,
}

#[derive(Clone, Copy, Debug)]
pub struct Header<'a> {
    pub key: &'a str,
    pub value: Option<&'a [u8]>,
}
/// A borrowed ingress descriptor. Copy admission retains only its accepted prefix.
#[derive(Clone, Copy, Debug)]
pub struct RecordDescriptor<'a> {
    pub topic: TopicHandle,
    pub partition_hint: Option<i32>,
    pub lane_hint: Option<u8>,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: &'a [Header<'a>],
    pub timestamp_ms: i64,
    pub user_token: u64,
    pub delivery_timeout: Option<RuntimeDuration>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn kafka_sequences_wrap_without_changing_identity() {
        assert!(Sequence::new(-1).is_none());
        assert_eq!(Sequence::new(i32::MAX).unwrap().advance(1), Sequence::ZERO);
        assert_eq!(Sequence::new(i32::MAX - 1).unwrap().advance(4).get(), 2);
        assert_eq!(Sequence::ZERO.advance(u32::MAX).get(), i32::MAX);
    }
    #[test]
    fn unobserved_transmission_is_never_an_acknowledgement() {
        for transmitted in [false, true] {
            let out = DeliveryOutcome::unresolved(transmitted, FailureReason::Deadline);
            assert_ne!(out.kind, DeliveryKind::Acked);
            assert_eq!(out.kind == DeliveryKind::Unknown, transmitted);
        }
    }
}
