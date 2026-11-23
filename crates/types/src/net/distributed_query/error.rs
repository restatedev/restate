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

/// Which admission failure a [`QueryTaskError`] reports, with its details.
/// Open-ended: unknown numbers are retained verbatim. `0` means none. Codes and
/// their payload semantics are permanent; never renumber or reuse them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, BilrostNewType)]
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

/// What kind of failure a [`QueryTaskError`] is, so that the coordinator and
/// its users can tell, for example, a bug from a storage condition. Open-ended: a
/// kind this node doesn't know is kept and shown like `UNKNOWN`. Never renumber
/// or reuse.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, BilrostNewType)]
pub struct QueryFailureKind(pub u32);

impl QueryFailureKind {
    /// Not classified, for example by a newer node.
    pub const UNKNOWN: Self = Self(0);
    /// A bug: the engine reached a state it shouldn't.
    pub const INTERNAL: Self = Self(1);
    /// Storage couldn't serve the read, for example a statistic that isn't complete.
    pub const STORAGE: Self = Self(2);
    /// The partition couldn't admit the read; the code says why.
    pub const ADMISSION: Self = Self(3);
    /// Executing the plan failed, for example on resource limits or bad data.
    pub const EXECUTION: Self = Self(4);
    /// The task was cancelled.
    pub const CANCELLED: Self = Self(5);
    /// The request doesn't fit the task's state, for example an unknown task.
    pub const PROTOCOL: Self = Self(6);

    fn name(self) -> Option<&'static str> {
        Some(match self {
            Self::INTERNAL => "internal",
            Self::STORAGE => "storage",
            Self::ADMISSION => "admission",
            Self::EXECUTION => "execution",
            Self::CANCELLED => "cancelled",
            Self::PROTOCOL => "protocol",
            _ => return None,
        })
    }
}

/// A failed task request: the node that raised it, its kind, the cause's own
/// message (without wrapper prefixes), and for admission failures a code and
/// details. Forwarding keeps it unchanged, including details a later release may
/// add, so read admission details through [`Self::partition_read_error`].
///
/// It displays as `<origin>: <kind>: <message>`, for example
/// `N2:1: storage: 'service_stats/1' is not complete on partition 12`; an unknown
/// kind is left out.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct QueryTaskError {
    #[bilrost(1)]
    pub origin: Option<GenerationalNodeId>,
    #[bilrost(2)]
    pub kind: QueryFailureKind,
    #[bilrost(3)]
    pub message: ReString,
    #[bilrost(4)]
    pub code: QueryErrorCode,
    #[bilrost(5)]
    pub details: Bytes,
}

impl QueryTaskError {
    /// A failure of `kind` with `message`; the node sending it sets the origin.
    pub fn new(kind: QueryFailureKind, message: impl Into<ReString>) -> Self {
        Self {
            origin: None,
            kind,
            message: message.into(),
            code: QueryErrorCode::default(),
            details: Bytes::new(),
        }
    }
}

impl std::fmt::Display for QueryTaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(origin) = self.origin {
            write!(f, "{origin}: ")?;
        }
        if let Some(kind) = self.kind.name() {
            write!(f, "{kind}: ")?;
        }
        f.write_str(&self.message)
    }
}

impl std::error::Error for QueryTaskError {}

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
    /// This admission failure as a task failure, with its code and details. The
    /// node sending it sets the origin.
    pub fn to_task_error(&self) -> QueryTaskError {
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
        QueryTaskError {
            origin: None,
            kind: QueryFailureKind::ADMISSION,
            message: self.to_string().into(),
            code,
            details,
        }
    }
}

impl QueryTaskError {
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
            let failure = error.to_task_error();
            assert_eq!(failure.code.0, code, "assigned_codes_are_permanent");
            assert_eq!(failure.kind, QueryFailureKind::ADMISSION);
            assert_eq!(failure.message, error.to_string());
            let decoded = QueryTaskError::decode(failure.encode_to_vec().as_slice()).unwrap();
            assert_eq!(decoded, failure);
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
        let metadata = QueryTaskError {
            code: QueryErrorCode::PARTITION_NOT_LEADER,
            details: FutureLeaderDetails {
                partition,
                leader,
                future_hint: Some("retained".into()),
            }
            .encode_to_vec()
            .into(),
            ..QueryTaskError::new(QueryFailureKind::ADMISSION, "leader moved")
        };
        assert_eq!(
            metadata.partition_read_error().unwrap(),
            Some(PartitionReadError::NotLeader { partition, leader })
        );
        let forwarded = QueryTaskError::decode(metadata.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            forwarded, metadata,
            "typed_views_do_not_reencode_and_drop_future_fields"
        );
        let future = FutureLeaderDetails::decode(forwarded.details).unwrap();
        assert_eq!(future.future_hint.as_deref(), Some("retained"));

        let without_hint = QueryTaskError {
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
        let unknown = QueryTaskError {
            origin: Some(GenerationalNodeId::new(3, 4)),
            kind: QueryFailureKind(999),
            message: "from the future".into(),
            code: QueryErrorCode(12345),
            // An unknown reason may use an entirely different payload format.
            details: Bytes::from_static(b"\xfffuture payload"),
        };
        assert_eq!(
            unknown.partition_read_error().unwrap(),
            None,
            "unknown_codes_are_not_decode_errors"
        );
        assert_eq!(
            QueryTaskError::decode(unknown.encode_to_vec().as_slice()).unwrap(),
            unknown
        );
        // An unknown kind is shown without a kind, like `UNKNOWN`.
        assert_eq!(unknown.to_string(), "N3:4: from the future");
        let storage = QueryTaskError {
            origin: Some(GenerationalNodeId::new(2, 1)),
            ..QueryTaskError::new(
                QueryFailureKind::STORAGE,
                "'service_stats/1' is not complete",
            )
        };
        assert_eq!(
            storage.to_string(),
            "N2:1: storage: 'service_stats/1' is not complete"
        );
        let bug = QueryTaskError::new(QueryFailureKind::INTERNAL, "unexpected plan");
        assert_eq!(bug.to_string(), "internal: unexpected plan");
    }
}
