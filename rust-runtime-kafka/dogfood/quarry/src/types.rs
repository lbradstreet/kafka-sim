use std::fmt;

use kr_runtime::RuntimeInstant;

/// Fixed memory and batching bounds for one queue engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueConfig {
    /// Maximum number of delayed, ready, and leased jobs combined.
    pub active_capacity: usize,
    /// Maximum payload size accepted by [`SubmitRequest`].
    pub max_payload_bytes: usize,
    /// Maximum number of jobs returned by one claim.
    pub max_claim_batch: usize,
    /// Number of completed jobs retained for submit and acknowledgement retries.
    pub completed_history_capacity: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            active_capacity: 1_024,
            max_payload_bytes: 64 * 1_024,
            max_claim_batch: 32,
            completed_history_capacity: 1_024,
        }
    }
}

macro_rules! identifier {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u64);

        impl $name {
            /// Constructs an identifier from its stable integer representation.
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Returns the stable integer representation.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

identifier!(RequestId, "A producer-supplied idempotency key.");
identifier!(JobId, "A queue-assigned job identifier.");
identifier!(WorkerId, "A caller-supplied worker identifier.");

/// A queue-assigned fencing token scoped to one broker incarnation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LeaseToken {
    incarnation: u64,
    sequence: u64,
}

impl LeaseToken {
    /// Constructs a token in the initial in-memory incarnation.
    ///
    /// Durable recovery uses [`Self::from_parts`] with a persisted incarnation.
    #[must_use]
    pub const fn new(sequence: u64) -> Self {
        Self::from_parts(0, sequence)
    }

    /// Constructs a token from its durable incarnation and local sequence.
    #[must_use]
    pub const fn from_parts(incarnation: u64, sequence: u64) -> Self {
        Self {
            incarnation,
            sequence,
        }
    }

    /// Returns the broker incarnation that issued this token.
    #[must_use]
    pub const fn incarnation(self) -> u64 {
        self.incarnation
    }

    /// Returns the token sequence within its broker incarnation.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

impl fmt::Display for LeaseToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.incarnation, self.sequence)
    }
}

/// A request to add one job to the queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmitRequest {
    pub request_id: RequestId,
    pub payload: Vec<u8>,
    pub not_before: RuntimeInstant,
}

/// The result of a successful submit operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitOutcome {
    /// A new active job was created.
    Submitted { job_id: JobId },
    /// The same request is already represented by an active job.
    DuplicateActive { job_id: JobId },
    /// The same request was completed and remains in bounded history.
    DuplicateCompleted { job_id: JobId },
}

impl SubmitOutcome {
    /// Returns the job assigned to either the new or deduplicated request.
    #[must_use]
    pub const fn job_id(self) -> JobId {
        match self {
            Self::Submitted { job_id }
            | Self::DuplicateActive { job_id }
            | Self::DuplicateCompleted { job_id } => job_id,
        }
    }
}

/// A job returned to a worker under a time-bounded lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeasedJob {
    pub request_id: RequestId,
    pub job_id: JobId,
    pub payload: Vec<u8>,
    pub worker_id: WorkerId,
    pub lease_token: LeaseToken,
    pub deadline: RuntimeInstant,
}

/// The result of acknowledging a leased job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckOutcome {
    /// The active job was completed by this call.
    Completed,
    /// The same acknowledgement was already completed and remains in history.
    AlreadyCompleted,
}

/// The result of renewing a leased job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenewOutcome {
    /// The lease now expires at `deadline`.
    Renewed { deadline: RuntimeInstant },
}

/// The result of negatively acknowledging a leased job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NackOutcome {
    /// The job is available at or after `available_at`.
    Requeued { available_at: RuntimeInstant },
}

/// A job's externally meaningful state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobStatus {
    Delayed {
        available_at: RuntimeInstant,
    },
    Ready,
    Leased {
        worker_id: WorkerId,
        lease_token: LeaseToken,
        deadline: RuntimeInstant,
    },
}

/// Semantic state for one active job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobSnapshot {
    pub request_id: RequestId,
    pub job_id: JobId,
    pub payload: Vec<u8>,
    pub status: JobStatus,
}

/// Semantic state retained for a completed job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedSnapshot {
    pub request_id: RequestId,
    pub job_id: JobId,
    pub payload: Vec<u8>,
    pub not_before: RuntimeInstant,
    pub ack_token: LeaseToken,
}

/// A semantic, implementation-independent view of queue state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueSnapshot {
    pub now: RuntimeInstant,
    pub active_capacity: usize,
    pub jobs: Vec<JobSnapshot>,
    pub completed: Vec<CompletedSnapshot>,
}

/// A rejected queue operation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueueError {
    PayloadTooLarge {
        size: usize,
        limit: usize,
    },
    ActiveCapacityReached {
        limit: usize,
    },
    RequestConflict {
        request_id: RequestId,
    },
    ClaimBatchTooLarge {
        requested: usize,
        limit: usize,
    },
    ZeroLeaseDuration,
    DeadlineOverflow,
    JobIdentifierExhausted,
    LeaseTokenExhausted,
    JobNotFound {
        job_id: JobId,
    },
    JobNotLeased {
        job_id: JobId,
    },
    StaleLeaseToken {
        job_id: JobId,
        provided: LeaseToken,
    },
    /// The broker's bounded command mailbox has no free slot.
    Backpressure {
        limit: usize,
    },
    /// The broker stopped before accepting or completing the command.
    BrokerStopped,
}

impl fmt::Display for QueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { size, limit } => {
                write!(formatter, "payload size {size} exceeds limit {limit}")
            }
            Self::ActiveCapacityReached { limit } => {
                write!(formatter, "active job capacity of {limit} was reached")
            }
            Self::RequestConflict { request_id } => {
                write!(
                    formatter,
                    "request id {request_id} was reused with different contents"
                )
            }
            Self::ClaimBatchTooLarge { requested, limit } => {
                write!(formatter, "claim batch {requested} exceeds limit {limit}")
            }
            Self::ZeroLeaseDuration => formatter.write_str("lease duration must be non-zero"),
            Self::DeadlineOverflow => formatter.write_str("queue deadline overflowed"),
            Self::JobIdentifierExhausted => formatter.write_str("job identifier space exhausted"),
            Self::LeaseTokenExhausted => formatter.write_str("lease token space exhausted"),
            Self::JobNotFound { job_id } => write!(formatter, "job {job_id} was not found"),
            Self::JobNotLeased { job_id } => write!(formatter, "job {job_id} is not leased"),
            Self::StaleLeaseToken { job_id, provided } => {
                write!(
                    formatter,
                    "lease token {provided} is stale for job {job_id}"
                )
            }
            Self::Backpressure { limit } => {
                write!(formatter, "broker command capacity of {limit} was reached")
            }
            Self::BrokerStopped => formatter.write_str("broker stopped"),
        }
    }
}

impl std::error::Error for QueueError {}
