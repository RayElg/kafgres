//! Codec-level failures, and the Kafka error codes clients drive retry logic from.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// Ran off the end of the buffer.
    Truncated { needed: usize, available: usize },
    /// A length prefix that cannot be honoured (negative, or absurdly large).
    InvalidLength(i64),
    /// A null where the negotiated version does not permit one.
    UnexpectedNull(&'static str),
    /// Strings on the wire are UTF-8.
    InvalidUtf8,
    /// Tagged fields must arrive in ascending tag order.
    TagOutOfOrder { previous: u32, tag: u32 },
    /// A varint that does not terminate within its width.
    MalformedVarint,
    /// Version outside the range the schema defines for this API.
    UnsupportedVersion { api_key: i16, version: i16 },
    /// An api key the vendored schemas do not define.
    UnknownApiKey(i16),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::Truncated { needed, available } => {
                write!(f, "truncated: needed {needed} bytes, {available} available")
            }
            CodecError::InvalidLength(n) => write!(f, "invalid length prefix {n}"),
            CodecError::UnexpectedNull(name) => write!(f, "unexpected null in field {name}"),
            CodecError::InvalidUtf8 => write!(f, "string field is not valid UTF-8"),
            CodecError::TagOutOfOrder { previous, tag } => {
                write!(f, "tagged field {tag} follows {previous}, not ascending")
            }
            CodecError::MalformedVarint => write!(f, "malformed varint"),
            CodecError::UnsupportedVersion { api_key, version } => {
                write!(f, "api {api_key} does not define version {version}")
            }
            CodecError::UnknownApiKey(k) => write!(f, "unknown api key {k}"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Kafka protocol error codes. Only the codes this broker can actually return are listed;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ErrorCode {
    UnknownServerError = -1,
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderOrFollower = 6,
    RequestTimedOut = 7,
    MessageTooLarge = 10,
    OffsetMetadataTooLarge = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    UnsupportedForMessageFormat = 43,
    PolicyViolation = 44,
    OutOfOrderSequenceNumber = 45,
    DuplicateSequenceNumber = 46,
    InvalidProducerEpoch = 47,
    InvalidTxnState = 48,
    InvalidProducerIdMapping = 49,
    InvalidTransactionTimeout = 50,
    ConcurrentTransactions = 51,
    TransactionCoordinatorFenced = 52,
    TransactionalIdAuthorizationFailed = 53,
    SecurityDisabled = 54,
    OperationNotAttempted = 55,
    KafkaStorageError = 56,
    LogDirNotFound = 57,
    SaslAuthenticationFailed = 58,
    UnknownProducerId = 59,
    ReassignmentInProgress = 60,
    /// The group has members. Kafka refuses to delete a live group rather than
    NonEmptyGroup = 68,
    GroupIdNotFound = 69,
    FencedLeaderEpoch = 74,
    UnknownLeaderEpoch = 75,
    UnsupportedCompressionType = 76,
    OffsetNotAvailable = 78,
    MemberIdRequired = 79,
    GroupMaxSizeReached = 81,
    /// The preferred replica is already the leader, so there is nothing to elect.
    ElectionNotNeeded = 84,
    /// The group still has members, so its committed offsets are in use. Kafka scopes this
    GroupSubscribedToTopic = 86,
    /// Terminal, unlike `CorruptMessage`. Upstream returns this for a batch that is
    InvalidRecord = 87,
    /// The named resource does not exist — used by `DescribeUserScramCredentials` for a
    ResourceNotFound = 91,
    UnacceptableCredential = 93,
    UnknownTopicId = 100,
    /// `DescribeTransactions` for a transactional id this broker has no state for. The
    TransactionalIdNotFound = 105,
    FencedMemberEpoch = 110,
    UnreleasedInstanceId = 111,
    UnsupportedAssignor = 112,
    StaleMemberEpoch = 113,
    InvalidRecordState = 121,
    ShareSessionNotFound = 122,
    InvalidShareSessionEpoch = 123,
    ShareSessionLimitReached = 133,
}

impl ErrorCode {
    pub fn code(self) -> i16 {
        self as i16
    }

    /// Whether the Java client treats this code as a `RetriableException`: every code
    pub fn is_retriable(self) -> bool {
        matches!(
            self,
            ErrorCode::CorruptMessage
                | ErrorCode::UnknownTopicOrPartition
                | ErrorCode::LeaderNotAvailable
                | ErrorCode::NotLeaderOrFollower
                | ErrorCode::RequestTimedOut
                | ErrorCode::NetworkException
                | ErrorCode::CoordinatorLoadInProgress
                | ErrorCode::CoordinatorNotAvailable
                | ErrorCode::NotCoordinator
                | ErrorCode::NotEnoughReplicas
                | ErrorCode::NotEnoughReplicasAfterAppend
                | ErrorCode::NotController
                | ErrorCode::ConcurrentTransactions
                | ErrorCode::KafkaStorageError
                | ErrorCode::FencedLeaderEpoch
                | ErrorCode::UnknownLeaderEpoch
                | ErrorCode::OffsetNotAvailable
                | ErrorCode::UnknownTopicId
        )
    }
}

impl From<CodecError> for ErrorCode {
    fn from(e: CodecError) -> Self {
        match e {
            CodecError::UnsupportedVersion { .. } | CodecError::UnknownApiKey(_) => {
                ErrorCode::UnsupportedVersion
            }
            CodecError::Truncated { .. }
            | CodecError::InvalidLength(_)
            | CodecError::UnexpectedNull(_)
            | CodecError::InvalidUtf8
            | CodecError::TagOutOfOrder { .. }
            | CodecError::MalformedVarint => ErrorCode::InvalidRequest,
        }
    }
}
