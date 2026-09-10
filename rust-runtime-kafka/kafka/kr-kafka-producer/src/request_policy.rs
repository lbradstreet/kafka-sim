//! Request grouping policy, independent of record-batch size and compression.

/// Controls which partition batches may share a Produce request. All variants
/// retain broker/lane boundaries, sequence fences and the exact request limits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RequestBatchingPolicy {
    /// Exactly one partition batch per request, including retries.
    SinglePartition,
    /// Gather already sealed, dispatchable heads. Preserves existing behavior.
    #[default]
    Sealed,
    /// A dispatchable head may also seal younger, fully encoded open heads on
    /// its broker/lane. One bounded candidate pass and one ordinary encoder
    /// pass are allowed before retrying dispatch. No additional linger or wait
    /// for new input/resources is introduced. A neighbor that is not finished
    /// in that pass remains independently eligible for a later request.
    BrokerReady,
}

impl RequestBatchingPolicy {
    pub(crate) fn partition_limit(self, configured: u16) -> usize {
        match self {
            Self::SinglePartition => 1,
            Self::Sealed | Self::BrokerReady => usize::from(configured),
        }
    }

    pub(crate) fn prepares_open(self) -> bool {
        matches!(self, Self::BrokerReady)
    }
}
