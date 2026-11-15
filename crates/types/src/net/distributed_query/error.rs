// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bilrost::{Message, OwnedMessage};
use bytes::Bytes;

use restate_encoding::BilrostNewType;
use restate_util_string::ReString;

use crate::GenerationalNodeId;
use crate::identifiers::PartitionId;

/// Open-ended query failure identity. Unknown numbers are retained verbatim.
/// Codes and their payload semantics are permanent; never renumber or reuse them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, BilrostNewType)]
pub struct QueryErrorCode(pub u32);

impl QueryErrorCode {
    pub const PARTITION_NOT_HOSTED: Self = Self(1);
    pub const PARTITION_INITIALIZING: Self = Self(2);
    pub const PARTITION_STOPPING: Self = Self(3);
    pub const PARTITION_BROKEN: Self = Self(4);
    pub const PARTITION_NOT_LEADER: Self = Self(5);
    pub const PARTITION_BUSY: Self = Self(6);
    pub const PARTITION_MISMATCH: Self = Self(7);
    pub const QUERY_MANAGER_CLOSED: Self = Self(8);
    pub const QUERY_ADMISSION_CLOSED: Self = Self(9);
    pub const QUERY_RESPONSE_DROPPED: Self = Self(10);
    pub const LIVE_CAPTURE_UNAVAILABLE: Self = Self(11);
}

/// Machine-readable context for a task's failure message: a code, its details
/// and the node that failed. Keep the original details when interpreting or
/// forwarding an error, also for known codes: later releases may add fields to
/// their detail messages.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct QueryErrorMetadata {
    #[bilrost(1)]
    pub code: QueryErrorCode,
    #[bilrost(2)]
    pub details: Bytes,
    #[bilrost(3)]
    pub origin: Option<GenerationalNodeId>,
}

/// A remote query failure, retaining metadata independently of typed decoding.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct QueryTaskError {
    pub message: ReString,
    pub metadata: Option<QueryErrorMetadata>,
}

/// Local admission outcomes. The enum itself is never encoded on the wire.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PartitionReadError {
    #[error("partition {0} is not hosted on this node; refresh partition placement")]
    NotHosted(PartitionId),
    #[error("partition {0} is initializing; retry after initialization completes")]
    Initializing(PartitionId),
    #[error("partition {0} is stopping; refresh partition placement")]
    Stopping(PartitionId),
    #[error("partition {partition} is broken and requires intervention: {reason}")]
    Broken {
        partition: PartitionId,
        reason: ReString,
    },
    #[error("not leader for partition {partition}; last observed leader: {}", leader.map_or_else(|| "unknown".to_owned(), |node| node.to_string()))]
    NotLeader {
        partition: PartitionId,
        leader: Option<GenerationalNodeId>,
    },
    #[error("partition {0} query admission queue is full; retry later")]
    Busy(PartitionId),
    #[error(
        "query read requested partition {requested} but reached processor for partition {actual}"
    )]
    PartitionMismatch {
        requested: PartitionId,
        actual: PartitionId,
    },
    #[error("query manager command channel is closed for partition {0}")]
    ManagerClosed(PartitionId),
    #[error("query admission channel is closed for partition {0}")]
    AdmissionClosed(PartitionId),
    #[error("query admission response was dropped for partition {0}")]
    ResponseDropped(PartitionId),
    #[error("requested live state is unavailable for partition {0}")]
    LiveCaptureUnavailable(PartitionId),
}

#[derive(bilrost::Message)]
struct PartitionDetails {
    #[bilrost(1)]
    partition: PartitionId,
}

#[derive(bilrost::Message)]
struct BrokenDetails {
    #[bilrost(1)]
    partition: PartitionId,
    #[bilrost(2)]
    reason: ReString,
}

#[derive(bilrost::Message)]
struct NotLeaderDetails {
    #[bilrost(1)]
    partition: PartitionId,
    #[bilrost(2)]
    leader: Option<GenerationalNodeId>,
}

#[derive(bilrost::Message)]
struct MismatchDetails {
    #[bilrost(1)]
    requested: PartitionId,
    #[bilrost(2)]
    actual: PartitionId,
}

impl PartitionReadError {
    pub fn to_metadata(&self) -> QueryErrorMetadata {
        use QueryErrorCode as Code;

        let partition_details = |partition: &PartitionId| {
            Bytes::from(
                PartitionDetails {
                    partition: *partition,
                }
                .encode_to_vec(),
            )
        };
        let (code, details) = match self {
            Self::NotHosted(partition) => {
                (Code::PARTITION_NOT_HOSTED, partition_details(partition))
            }
            Self::Initializing(partition) => {
                (Code::PARTITION_INITIALIZING, partition_details(partition))
            }
            Self::Stopping(partition) => (Code::PARTITION_STOPPING, partition_details(partition)),
            Self::Busy(partition) => (Code::PARTITION_BUSY, partition_details(partition)),
            Self::ManagerClosed(partition) => {
                (Code::QUERY_MANAGER_CLOSED, partition_details(partition))
            }
            Self::AdmissionClosed(partition) => {
                (Code::QUERY_ADMISSION_CLOSED, partition_details(partition))
            }
            Self::ResponseDropped(partition) => {
                (Code::QUERY_RESPONSE_DROPPED, partition_details(partition))
            }
            Self::LiveCaptureUnavailable(partition) => {
                (Code::LIVE_CAPTURE_UNAVAILABLE, partition_details(partition))
            }
            Self::Broken { partition, reason } => (
                Code::PARTITION_BROKEN,
                BrokenDetails {
                    partition: *partition,
                    reason: reason.clone(),
                }
                .encode_to_vec()
                .into(),
            ),
            Self::NotLeader { partition, leader } => (
                Code::PARTITION_NOT_LEADER,
                NotLeaderDetails {
                    partition: *partition,
                    leader: *leader,
                }
                .encode_to_vec()
                .into(),
            ),
            Self::PartitionMismatch { requested, actual } => (
                Code::PARTITION_MISMATCH,
                MismatchDetails {
                    requested: *requested,
                    actual: *actual,
                }
                .encode_to_vec()
                .into(),
            ),
        };
        QueryErrorMetadata {
            code,
            details,
            origin: None,
        }
    }
}

impl QueryErrorMetadata {
    /// Interprets known admission failures without consuming their original bytes.
    /// Unknown codes require no payload decoding and are not protocol errors.
    pub fn partition_read_error(&self) -> Result<Option<PartitionReadError>, bilrost::DecodeError> {
        use PartitionReadError as Error;
        use QueryErrorCode as Code;

        let partition =
            || PartitionDetails::decode(self.details.as_ref()).map(|value| value.partition);
        Ok(Some(match self.code {
            Code::PARTITION_NOT_HOSTED => Error::NotHosted(partition()?),
            Code::PARTITION_INITIALIZING => Error::Initializing(partition()?),
            Code::PARTITION_STOPPING => Error::Stopping(partition()?),
            Code::PARTITION_BUSY => Error::Busy(partition()?),
            Code::QUERY_MANAGER_CLOSED => Error::ManagerClosed(partition()?),
            Code::QUERY_ADMISSION_CLOSED => Error::AdmissionClosed(partition()?),
            Code::QUERY_RESPONSE_DROPPED => Error::ResponseDropped(partition()?),
            Code::LIVE_CAPTURE_UNAVAILABLE => Error::LiveCaptureUnavailable(partition()?),
            Code::PARTITION_BROKEN => {
                let value = BrokenDetails::decode(self.details.as_ref())?;
                Error::Broken {
                    partition: value.partition,
                    reason: value.reason,
                }
            }
            Code::PARTITION_NOT_LEADER => {
                let value = NotLeaderDetails::decode(self.details.as_ref())?;
                Error::NotLeader {
                    partition: value.partition,
                    leader: value.leader,
                }
            }
            Code::PARTITION_MISMATCH => {
                let value = MismatchDetails::decode(self.details.as_ref())?;
                Error::PartitionMismatch {
                    requested: value.requested,
                    actual: value.actual,
                }
            }
            _ => return Ok(None),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_details_are_typed_views_over_extensible_payloads() {
        let partition = PartitionId::from(7);
        for (error, code) in [
            (PartitionReadError::NotHosted(partition), 1),
            (PartitionReadError::Initializing(partition), 2),
            (PartitionReadError::LiveCaptureUnavailable(partition), 11),
            (PartitionReadError::Stopping(partition), 3),
            (
                PartitionReadError::Broken {
                    partition,
                    reason: "ahead of log".into(),
                },
                4,
            ),
            (
                PartitionReadError::NotLeader {
                    partition,
                    leader: None,
                },
                5,
            ),
            (
                PartitionReadError::NotLeader {
                    partition,
                    leader: Some(GenerationalNodeId::new(2, 3)),
                },
                5,
            ),
            (PartitionReadError::Busy(partition), 6),
            (
                PartitionReadError::PartitionMismatch {
                    requested: partition,
                    actual: PartitionId::MIN,
                },
                7,
            ),
            (PartitionReadError::ManagerClosed(partition), 8),
            (PartitionReadError::AdmissionClosed(partition), 9),
            (PartitionReadError::ResponseDropped(partition), 10),
        ] {
            let metadata = error.to_metadata();
            assert_eq!(metadata.code.0, code, "assigned_codes_are_permanent");
            let decoded = QueryErrorMetadata::decode(metadata.encode_to_vec().as_slice()).unwrap();
            assert_eq!(decoded, metadata);
            assert_eq!(decoded.partition_read_error().unwrap(), Some(error));
        }

        #[derive(bilrost::Message)]
        struct FutureLeaderDetails {
            #[bilrost(1)]
            partition: PartitionId,
            #[bilrost(2)]
            leader: Option<GenerationalNodeId>,
            #[bilrost(100)]
            future_hint: Option<ReString>,
        }
        let leader = Some(GenerationalNodeId::new(4, 5));
        let metadata = QueryErrorMetadata {
            code: QueryErrorCode::PARTITION_NOT_LEADER,
            details: FutureLeaderDetails {
                partition,
                leader,
                future_hint: Some("retained".into()),
            }
            .encode_to_vec()
            .into(),
            origin: None,
        };
        assert_eq!(
            metadata.partition_read_error().unwrap(),
            Some(PartitionReadError::NotLeader { partition, leader })
        );
        let forwarded = QueryErrorMetadata::decode(metadata.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            forwarded, metadata,
            "typed_views_do_not_reencode_and_drop_future_fields"
        );
        let future = FutureLeaderDetails::decode(forwarded.details).unwrap();
        assert_eq!(future.future_hint.as_deref(), Some("retained"));

        let without_hint = QueryErrorMetadata {
            details: PartitionDetails { partition }.encode_to_vec().into(),
            ..metadata
        };
        assert_eq!(
            without_hint.partition_read_error().unwrap(),
            Some(PartitionReadError::NotLeader {
                partition,
                leader: None
            }),
            "absent_optional_hints_remain_unknown"
        );
        let unknown = QueryErrorMetadata {
            code: QueryErrorCode(12345),
            // An unknown reason may use an entirely different payload format.
            details: Bytes::from_static(b"\xfffuture payload"),
            origin: Some(GenerationalNodeId::new(3, 4)),
        };
        assert_eq!(
            unknown.partition_read_error().unwrap(),
            None,
            "unknown_codes_are_not_decode_errors"
        );
        assert_eq!(
            QueryErrorMetadata::decode(unknown.encode_to_vec().as_slice()).unwrap(),
            unknown
        );
    }
}
