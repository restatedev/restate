// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::Version;
use crate::identifiers::{PartitionId, SnapshotId};
use crate::logs::{LogId, Lsn};
use crate::net::{
    ServiceTag, bilrost_wire_codec, default_wire_codec, define_rpc, define_service,
    define_unary_message,
};

pub struct PartitionManagerService;

define_service! {
    @service = PartitionManagerService,
    @tag = ServiceTag::PartitionManagerService,
}

define_unary_message! {
    @message = ControlProcessors,
    @service = PartitionManagerService,
}

default_wire_codec!(ControlProcessors);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlProcessors {
    pub min_partition_table_version: Version,
    pub min_logs_table_version: Version,
    pub commands: Vec<ControlProcessor>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ControlProcessor {
    pub partition_id: PartitionId,
    pub command: ProcessorCommand,
    // Version of the current partition configuration used for creating the command for selecting
    // the leader. Restate <= 1.3.2 does not set the current version attribute.
    #[serde(default = "Version::invalid")]
    pub current_version: Version,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, derive_more::Display)]
pub enum ProcessorCommand {
    // #[deprecated(
    //     since = "1.3.3",
    //     note = "Stopping should happen based on the PartitionReplicaSetStates"
    // )]
    Stop,
    // #[deprecated(
    //     since = "1.3.3",
    //     note = "Starting followers should happen based on the PartitionReplicaSetStates"
    // )]
    Follower,
    Leader,
}

define_rpc! {
    @request = CreateSnapshotRequest,
    @response = CreateSnapshotResponse,
    @service = PartitionManagerService,
}

default_wire_codec!(CreateSnapshotRequest);
default_wire_codec!(CreateSnapshotResponse);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotRequest {
    pub partition_id: PartitionId,
    pub min_target_lsn: Option<Lsn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotResponse {
    pub result: Result<Snapshot, SnapshotError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_id: SnapshotId,
    pub log_id: LogId,
    pub min_applied_lsn: Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotError {
    SnapshotCreationFailed(String),
}

define_rpc! {
    @request = DropPartitionStoreRequest,
    @response = DropPartitionStoreResponse,
    @service = PartitionManagerService,
}

bilrost_wire_codec!(DropPartitionStoreRequest);
bilrost_wire_codec!(DropPartitionStoreResponse);

/// Asks a node to delete its local copy of a partition's data.
///
/// Without `force`, the node only deletes a store that no processor uses: either the node has given
/// up on running the partition (see [`crate::cluster::cluster_state::BrokenReason`]), or it is
/// neither running the partition nor part of its replica set. That way a replica never loses its
/// store, even while its processor is restarting. `force` skips these checks and stops a running
/// processor first.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct DropPartitionStoreRequest {
    #[bilrost(1)]
    pub partition_id: PartitionId,
    #[bilrost(2)]
    pub force: bool,
    /// The newest partition configuration version that the requester based its decision on.
    /// Without `force`, a node that doesn't know this version yet waits a few seconds for it, and
    /// refuses if it doesn't arrive. That way the node doesn't act on an older replica set than
    /// the requester saw. [`Version::INVALID`] doesn't wait.
    #[bilrost(3)]
    pub min_partition_configuration_version: Version,
    /// Why the store is being dropped, as given by the operator. Logged for auditing.
    #[bilrost(4)]
    pub reason: String,
    /// Only report what would happen, without changing anything. The node makes the same
    /// checks, and answers with the store it would drop and its disk usage.
    #[bilrost(5)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Oneof, bilrost::Message)]
pub enum DropPartitionStoreResponse {
    #[bilrost(empty)]
    Unknown,
    #[bilrost(tag(1), message)]
    Dropped {
        #[bilrost(1)]
        deleted_bytes: Option<u64>,
        #[bilrost(2)]
        dry_run: bool,
    },
    #[bilrost(tag(2), message)]
    NoStoreFound {
        #[bilrost(1)]
        dry_run: bool,
    },
    #[bilrost(tag(3), message)]
    ProcessorRunning,
    #[bilrost(tag(4), message)]
    DropInProgress,
    #[bilrost(tag(5), message)]
    UnknownPartition,
    #[bilrost(tag(6))]
    Internal(String),
    #[bilrost(tag(7), message)]
    ReplicaSetMember,
    #[bilrost(tag(8), message)]
    PartitionConfigurationNotObserved {
        #[bilrost(1)]
        required: Version,
        #[bilrost(2)]
        observed: Version,
    },
}

impl DropPartitionStoreResponse {
    pub fn into_result(self) -> Result<DropPartitionStoreOutcome, DropPartitionStoreError> {
        match self {
            Self::Unknown => Err(DropPartitionStoreError::Internal(
                "the node returned an unknown response".to_owned(),
            )),
            Self::Dropped {
                deleted_bytes,
                dry_run,
            } => Ok(DropPartitionStoreOutcome::Dropped {
                deleted_bytes,
                dry_run,
            }),
            Self::NoStoreFound { dry_run } => {
                Ok(DropPartitionStoreOutcome::NoStoreFound { dry_run })
            }
            Self::ProcessorRunning => Err(DropPartitionStoreError::ProcessorRunning),
            Self::DropInProgress => Err(DropPartitionStoreError::DropInProgress),
            Self::UnknownPartition => Err(DropPartitionStoreError::UnknownPartition),
            Self::Internal(message) => Err(DropPartitionStoreError::Internal(message)),
            Self::ReplicaSetMember => Err(DropPartitionStoreError::ReplicaSetMember),
            Self::PartitionConfigurationNotObserved { required, observed } => {
                Err(DropPartitionStoreError::PartitionConfigurationNotObserved {
                    required,
                    observed,
                })
            }
        }
    }
}

impl From<Result<DropPartitionStoreOutcome, DropPartitionStoreError>>
    for DropPartitionStoreResponse
{
    fn from(result: Result<DropPartitionStoreOutcome, DropPartitionStoreError>) -> Self {
        match result {
            Ok(DropPartitionStoreOutcome::Dropped {
                deleted_bytes,
                dry_run,
            }) => Self::Dropped {
                deleted_bytes,
                dry_run,
            },
            Ok(DropPartitionStoreOutcome::NoStoreFound { dry_run }) => {
                Self::NoStoreFound { dry_run }
            }
            Err(DropPartitionStoreError::ProcessorRunning) => Self::ProcessorRunning,
            Err(DropPartitionStoreError::DropInProgress) => Self::DropInProgress,
            Err(DropPartitionStoreError::UnknownPartition) => Self::UnknownPartition,
            Err(DropPartitionStoreError::Internal(message)) => Self::Internal(message),
            Err(DropPartitionStoreError::ReplicaSetMember) => Self::ReplicaSetMember,
            Err(DropPartitionStoreError::PartitionConfigurationNotObserved {
                required,
                observed,
            }) => Self::PartitionConfigurationNotObserved { required, observed },
        }
    }
}

/// What a node did with a drop request that it accepted.
///
/// `dry_run` says whether the node handled the request as a dry run, without changing anything.
/// A node that doesn't support dry runs never sets it, so if a dry run was requested but this
/// is false, the node really dropped the store.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, derive_more::Display)]
pub enum DropPartitionStoreOutcome {
    /// The store was dropped, or in a dry run, would be dropped.
    #[display("the local partition store was dropped")]
    Dropped {
        /// How much disk space the store used right before it was deleted, if known.
        deleted_bytes: Option<u64>,
        dry_run: bool,
    },
    #[display("this node has no local partition store for this partition")]
    NoStoreFound { dry_run: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::Display)]
pub enum DropPartitionStoreError {
    #[display(
        "a partition processor is running on this node and the partition does not look broken"
    )]
    ProcessorRunning,
    #[display("another drop request for this partition is already in progress")]
    DropInProgress,
    #[display("the partition is not in this node's partition table")]
    UnknownPartition,
    #[display("failed to drop the local partition store: {_0}")]
    Internal(String),
    #[display("this node is part of the partition's replica set")]
    ReplicaSetMember,
    #[display(
        "this node hasn't learned about partition configuration {required} yet, it only knows \
        {observed}"
    )]
    PartitionConfigurationNotObserved {
        required: Version,
        observed: Version,
    },
}

#[cfg(test)]
mod tests {
    use bilrost::{Message, OwnedMessage};

    use super::*;

    #[test]
    fn drop_partition_store_messages_round_trip() {
        let request = DropPartitionStoreRequest {
            partition_id: PartitionId::new_unchecked(42),
            force: true,
            min_partition_configuration_version: Version::from(7),
            reason: "stale store".to_owned(),
            dry_run: true,
        };
        assert_eq!(
            DropPartitionStoreRequest::decode(request.encode_to_bytes()).unwrap(),
            request
        );

        for response in [
            DropPartitionStoreResponse::Unknown,
            DropPartitionStoreResponse::Dropped {
                deleted_bytes: Some(1024),
                dry_run: false,
            },
            DropPartitionStoreResponse::Dropped {
                deleted_bytes: None,
                dry_run: true,
            },
            DropPartitionStoreResponse::NoStoreFound { dry_run: true },
            DropPartitionStoreResponse::ProcessorRunning,
            DropPartitionStoreResponse::DropInProgress,
            DropPartitionStoreResponse::UnknownPartition,
            DropPartitionStoreResponse::Internal("failed".to_owned()),
            DropPartitionStoreResponse::ReplicaSetMember,
            DropPartitionStoreResponse::PartitionConfigurationNotObserved {
                required: Version::from(7),
                observed: Version::from(5),
            },
        ] {
            assert_eq!(
                DropPartitionStoreResponse::decode(response.encode_to_bytes()).unwrap(),
                response
            );
        }
    }
}
