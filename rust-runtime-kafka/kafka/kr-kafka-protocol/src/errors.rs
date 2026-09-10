//! Pinned Kafka error codes and the nontransactional producer's response policy.
//!
//! This is the allowlist in `producer_design.md` sections 23 and 24. Codes
//! outside that Produce policy, including known codes used by other Kafka APIs,
//! fail the producer closed. Kafka's Java `RetriableException` hierarchy is not
//! a substitute for the producer's sequence and delivery-certainty rules.
//!
//! Error numbers come from `Errors.java`, not the protocol JSON schemas. The
//! complete pinned Java source is retained as an independent test fixture.

/// Immutable Apache Kafka source revision shared with `schemas/PROVENANCE.lock`.
pub const ERROR_SOURCE_REVISION: &str = "7be741d08b3b06f6414ac868e57bf9b958f53a72";
/// SHA-256 of the exact checked-in upstream `Errors.java` fixture.
pub const ERROR_SOURCE_SHA256: &str =
    "5a1a3184d1403cb61893fecae0ccf2185c4b61fd089262fd9c0978df51054428";

/// Required producer action after a completely validated partition response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorClass {
    /// Acknowledged; the response can carry an offset and timestamp.
    Success,
    /// Acknowledged duplicate; do not report a new base offset.
    DuplicateSequence,
    /// Retry the identical batch with unchanged identity and sequence.
    Retry,
    /// Retry unchanged and refresh metadata by the retained topic ID.
    RefreshMetadata,
    /// Refresh the retained ID; confirmed deletion terminalizes only unsent work.
    RefreshTopicId,
    /// Fail the topic closed; retain ambiguity for already transmitted work.
    TopicFatal,
    /// The ordered ledger decides whether to hold, bump epoch, or fail closed.
    SequenceRecovery,
    /// This partition response definitively rejects the batch without writing it.
    DefinitiveNotWritten,
    /// Fail the producer closed, preserving prior transmission uncertainty.
    ProducerFatal,
    /// Unrecognized wire code: fail closed and never treat it as success.
    UnknownFatal,
}

impl ErrorClass {
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Success | Self::DuplicateSequence)
    }

    #[must_use]
    pub const fn is_retriable(self) -> bool {
        matches!(
            self,
            Self::Retry | Self::RefreshMetadata | Self::RefreshTopicId
        )
    }

    #[must_use]
    pub const fn requires_metadata_refresh(self) -> bool {
        matches!(self, Self::RefreshMetadata | Self::RefreshTopicId)
    }

    #[must_use]
    pub const fn is_producer_fatal(self) -> bool {
        matches!(self, Self::ProducerFatal | Self::UnknownFatal)
    }
}

/// One explicit classification from the complete pinned upstream inventory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ErrorDefinition {
    pub code: i16,
    pub name: &'static str,
    pub class: ErrorClass,
}

macro_rules! error_table {
    ($($name:ident = $code:literal => $class:ident,)*) => {
        $(pub const $name: i16 = $code;)*
        /// Every error defined by the pinned upstream source, in code order.
        pub const ALL_ERRORS: &[ErrorDefinition] = &[
            $(ErrorDefinition { code: $code, name: stringify!($name), class: ErrorClass::$class },)*
        ];

        /// Classifies a partition response using the explicit v0 producer policy.
        #[must_use]
        pub const fn classify(code: i16) -> ErrorClass {
            match code {
                $($code => ErrorClass::$class,)*
                _ => ErrorClass::UnknownFatal,
            }
        }
    };
}

// Every row is intentional. New upstream codes must be reviewed and classified;
// the fixture inventory test prevents a source bump from silently adding codes.
error_table! {
    UNKNOWN_SERVER_ERROR = -1 => ProducerFatal,
    NONE = 0 => Success,
    OFFSET_OUT_OF_RANGE = 1 => ProducerFatal,
    CORRUPT_MESSAGE = 2 => DefinitiveNotWritten,
    UNKNOWN_TOPIC_OR_PARTITION = 3 => RefreshMetadata,
    INVALID_FETCH_SIZE = 4 => ProducerFatal,
    LEADER_NOT_AVAILABLE = 5 => RefreshMetadata,
    NOT_LEADER_OR_FOLLOWER = 6 => RefreshMetadata,
    REQUEST_TIMED_OUT = 7 => Retry,
    BROKER_NOT_AVAILABLE = 8 => ProducerFatal,
    REPLICA_NOT_AVAILABLE = 9 => ProducerFatal,
    MESSAGE_TOO_LARGE = 10 => DefinitiveNotWritten,
    STALE_CONTROLLER_EPOCH = 11 => ProducerFatal,
    OFFSET_METADATA_TOO_LARGE = 12 => ProducerFatal,
    NETWORK_EXCEPTION = 13 => ProducerFatal,
    COORDINATOR_LOAD_IN_PROGRESS = 14 => ProducerFatal,
    COORDINATOR_NOT_AVAILABLE = 15 => ProducerFatal,
    NOT_COORDINATOR = 16 => ProducerFatal,
    INVALID_TOPIC_EXCEPTION = 17 => ProducerFatal,
    RECORD_LIST_TOO_LARGE = 18 => DefinitiveNotWritten,
    NOT_ENOUGH_REPLICAS = 19 => Retry,
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20 => Retry,
    INVALID_REQUIRED_ACKS = 21 => DefinitiveNotWritten,
    ILLEGAL_GENERATION = 22 => ProducerFatal,
    INCONSISTENT_GROUP_PROTOCOL = 23 => ProducerFatal,
    INVALID_GROUP_ID = 24 => ProducerFatal,
    UNKNOWN_MEMBER_ID = 25 => ProducerFatal,
    INVALID_SESSION_TIMEOUT = 26 => ProducerFatal,
    REBALANCE_IN_PROGRESS = 27 => ProducerFatal,
    INVALID_COMMIT_OFFSET_SIZE = 28 => ProducerFatal,
    TOPIC_AUTHORIZATION_FAILED = 29 => DefinitiveNotWritten,
    GROUP_AUTHORIZATION_FAILED = 30 => ProducerFatal,
    CLUSTER_AUTHORIZATION_FAILED = 31 => ProducerFatal,
    INVALID_TIMESTAMP = 32 => ProducerFatal,
    UNSUPPORTED_SASL_MECHANISM = 33 => ProducerFatal,
    ILLEGAL_SASL_STATE = 34 => ProducerFatal,
    UNSUPPORTED_VERSION = 35 => ProducerFatal,
    TOPIC_ALREADY_EXISTS = 36 => ProducerFatal,
    INVALID_PARTITIONS = 37 => ProducerFatal,
    INVALID_REPLICATION_FACTOR = 38 => ProducerFatal,
    INVALID_REPLICA_ASSIGNMENT = 39 => ProducerFatal,
    INVALID_CONFIG = 40 => ProducerFatal,
    NOT_CONTROLLER = 41 => ProducerFatal,
    INVALID_REQUEST = 42 => ProducerFatal,
    UNSUPPORTED_FOR_MESSAGE_FORMAT = 43 => DefinitiveNotWritten,
    POLICY_VIOLATION = 44 => ProducerFatal,
    OUT_OF_ORDER_SEQUENCE_NUMBER = 45 => SequenceRecovery,
    DUPLICATE_SEQUENCE_NUMBER = 46 => DuplicateSequence,
    INVALID_PRODUCER_EPOCH = 47 => ProducerFatal,
    INVALID_TXN_STATE = 48 => ProducerFatal,
    INVALID_PRODUCER_ID_MAPPING = 49 => ProducerFatal,
    INVALID_TRANSACTION_TIMEOUT = 50 => ProducerFatal,
    CONCURRENT_TRANSACTIONS = 51 => ProducerFatal,
    TRANSACTION_COORDINATOR_FENCED = 52 => ProducerFatal,
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53 => ProducerFatal,
    SECURITY_DISABLED = 54 => ProducerFatal,
    OPERATION_NOT_ATTEMPTED = 55 => ProducerFatal,
    KAFKA_STORAGE_ERROR = 56 => Retry,
    LOG_DIR_NOT_FOUND = 57 => ProducerFatal,
    SASL_AUTHENTICATION_FAILED = 58 => ProducerFatal,
    UNKNOWN_PRODUCER_ID = 59 => SequenceRecovery,
    REASSIGNMENT_IN_PROGRESS = 60 => ProducerFatal,
    DELEGATION_TOKEN_AUTH_DISABLED = 61 => ProducerFatal,
    DELEGATION_TOKEN_NOT_FOUND = 62 => ProducerFatal,
    DELEGATION_TOKEN_OWNER_MISMATCH = 63 => ProducerFatal,
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64 => ProducerFatal,
    DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65 => ProducerFatal,
    DELEGATION_TOKEN_EXPIRED = 66 => ProducerFatal,
    INVALID_PRINCIPAL_TYPE = 67 => ProducerFatal,
    NON_EMPTY_GROUP = 68 => ProducerFatal,
    GROUP_ID_NOT_FOUND = 69 => ProducerFatal,
    FETCH_SESSION_ID_NOT_FOUND = 70 => ProducerFatal,
    INVALID_FETCH_SESSION_EPOCH = 71 => ProducerFatal,
    LISTENER_NOT_FOUND = 72 => ProducerFatal,
    TOPIC_DELETION_DISABLED = 73 => ProducerFatal,
    FENCED_LEADER_EPOCH = 74 => RefreshMetadata,
    UNKNOWN_LEADER_EPOCH = 75 => RefreshMetadata,
    UNSUPPORTED_COMPRESSION_TYPE = 76 => DefinitiveNotWritten,
    STALE_BROKER_EPOCH = 77 => ProducerFatal,
    OFFSET_NOT_AVAILABLE = 78 => ProducerFatal,
    MEMBER_ID_REQUIRED = 79 => ProducerFatal,
    PREFERRED_LEADER_NOT_AVAILABLE = 80 => ProducerFatal,
    GROUP_MAX_SIZE_REACHED = 81 => ProducerFatal,
    FENCED_INSTANCE_ID = 82 => ProducerFatal,
    ELIGIBLE_LEADERS_NOT_AVAILABLE = 83 => ProducerFatal,
    ELECTION_NOT_NEEDED = 84 => ProducerFatal,
    NO_REASSIGNMENT_IN_PROGRESS = 85 => ProducerFatal,
    GROUP_SUBSCRIBED_TO_TOPIC = 86 => ProducerFatal,
    INVALID_RECORD = 87 => DefinitiveNotWritten,
    UNSTABLE_OFFSET_COMMIT = 88 => ProducerFatal,
    THROTTLING_QUOTA_EXCEEDED = 89 => ProducerFatal,
    PRODUCER_FENCED = 90 => ProducerFatal,
    RESOURCE_NOT_FOUND = 91 => ProducerFatal,
    DUPLICATE_RESOURCE = 92 => ProducerFatal,
    UNACCEPTABLE_CREDENTIAL = 93 => ProducerFatal,
    INCONSISTENT_VOTER_SET = 94 => ProducerFatal,
    INVALID_UPDATE_VERSION = 95 => ProducerFatal,
    FEATURE_UPDATE_FAILED = 96 => ProducerFatal,
    PRINCIPAL_DESERIALIZATION_FAILURE = 97 => ProducerFatal,
    SNAPSHOT_NOT_FOUND = 98 => ProducerFatal,
    POSITION_OUT_OF_RANGE = 99 => ProducerFatal,
    UNKNOWN_TOPIC_ID = 100 => RefreshTopicId,
    DUPLICATE_BROKER_REGISTRATION = 101 => ProducerFatal,
    BROKER_ID_NOT_REGISTERED = 102 => ProducerFatal,
    INCONSISTENT_TOPIC_ID = 103 => TopicFatal,
    INCONSISTENT_CLUSTER_ID = 104 => ProducerFatal,
    TRANSACTIONAL_ID_NOT_FOUND = 105 => ProducerFatal,
    FETCH_SESSION_TOPIC_ID_ERROR = 106 => ProducerFatal,
    INELIGIBLE_REPLICA = 107 => ProducerFatal,
    NEW_LEADER_ELECTED = 108 => ProducerFatal,
    OFFSET_MOVED_TO_TIERED_STORAGE = 109 => ProducerFatal,
    FENCED_MEMBER_EPOCH = 110 => ProducerFatal,
    UNRELEASED_INSTANCE_ID = 111 => ProducerFatal,
    UNSUPPORTED_ASSIGNOR = 112 => ProducerFatal,
    STALE_MEMBER_EPOCH = 113 => ProducerFatal,
    MISMATCHED_ENDPOINT_TYPE = 114 => ProducerFatal,
    UNSUPPORTED_ENDPOINT_TYPE = 115 => ProducerFatal,
    UNKNOWN_CONTROLLER_ID = 116 => ProducerFatal,
    UNKNOWN_SUBSCRIPTION_ID = 117 => ProducerFatal,
    TELEMETRY_TOO_LARGE = 118 => ProducerFatal,
    INVALID_REGISTRATION = 119 => ProducerFatal,
    TRANSACTION_ABORTABLE = 120 => ProducerFatal,
    INVALID_RECORD_STATE = 121 => ProducerFatal,
    SHARE_SESSION_NOT_FOUND = 122 => ProducerFatal,
    INVALID_SHARE_SESSION_EPOCH = 123 => ProducerFatal,
    FENCED_STATE_EPOCH = 124 => ProducerFatal,
    INVALID_VOTER_KEY = 125 => ProducerFatal,
    DUPLICATE_VOTER = 126 => ProducerFatal,
    VOTER_NOT_FOUND = 127 => ProducerFatal,
    INVALID_REGULAR_EXPRESSION = 128 => ProducerFatal,
    REBOOTSTRAP_REQUIRED = 129 => ProducerFatal,
    STREAMS_INVALID_TOPOLOGY = 130 => ProducerFatal,
    STREAMS_INVALID_TOPOLOGY_EPOCH = 131 => ProducerFatal,
    STREAMS_TOPOLOGY_FENCED = 132 => ProducerFatal,
    SHARE_SESSION_LIMIT_REACHED = 133 => ProducerFatal,
    GROUP_DELETION_FAILED = 134 => ProducerFatal,
    STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED = 135 => ProducerFatal,
}

/// Looks up a pinned code without allocating; unknown codes remain unknown.
#[must_use]
pub fn lookup(code: i16) -> Option<&'static ErrorDefinition> {
    ALL_ERRORS
        .binary_search_by_key(&code, |entry| entry.code)
        .ok()
        .map(|index| &ALL_ERRORS[index])
}

/// Kafka delivery certainty, distinct from transport-operation certainty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum DeliveryCertainty {
    Acked = 0,
    NotWritten = 1,
    Unknown = 2,
}

/// Classifies a terminal batch from its complete attempt history.
///
/// `transmitted` is sticky across every fragment, attempt, reconnect, and leader
/// change. `acknowledged` means a previously parsed successful partition response
/// matched this batch; a completed socket write never sets it. `terminal_response`
/// is a parsed response for this batch, not a fatal error from another partition.
/// Supply `None` for a timeout, local cancellation, or collateral fail-closed.
///
/// The caller must first apply the ordered ledger's section 18.4 rule before
/// passing a `SequenceRecovery` response as terminal. Neither this helper nor an
/// error code alone authorizes an epoch bump. A metadata deletion response does
/// not establish that a prior ambiguous Produce attempt was never committed.
#[must_use]
pub const fn terminal_certainty(
    transmitted: bool,
    acknowledged: bool,
    terminal_response: Option<i16>,
) -> DeliveryCertainty {
    if acknowledged {
        return DeliveryCertainty::Acked;
    }
    if let Some(code) = terminal_response {
        match classify(code) {
            ErrorClass::Success | ErrorClass::DuplicateSequence => return DeliveryCertainty::Acked,
            ErrorClass::DefinitiveNotWritten | ErrorClass::SequenceRecovery => {
                return DeliveryCertainty::NotWritten;
            }
            _ => return DeliveryCertainty::Unknown,
        }
    }
    if transmitted {
        DeliveryCertainty::Unknown
    } else {
        DeliveryCertainty::NotWritten
    }
}
