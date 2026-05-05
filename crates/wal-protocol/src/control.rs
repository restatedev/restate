// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::fsm_table::{CurrentReplicaSetState, NextReplicaSetState};
use restate_types::identifiers::{LeaderEpoch, PartitionId};
use restate_types::logs::{Keys, Lsn, SequenceNumber};
use restate_types::partitions::PartitionConfiguration;
use restate_types::partitions::state::{MemberState, ReplicaSetState};
use restate_types::replication::{NodeSet, ReplicationProperty};
use restate_types::schema::Schema;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::{
    GenerationalNodeId, SemanticRestateVersion, Version, Versioned, bilrost_storage_encode_decode,
    flexbuffers_storage_encode_decode,
};

/// Announcing a new leader. This message can be written by any component to make the specified
/// partition processor the leader.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct AnnounceLeader {
    /// Sender of the announce leader message.
    ///
    /// This became non-optional in v1.5. Noting that it has always been set in previous versions,
    /// it's safe to assume that it's always set.
    #[bilrost(tag(1))]
    pub node_id: GenerationalNodeId,
    #[bilrost(tag(2))]
    pub leader_epoch: LeaderEpoch,
    #[bilrost(tag(3))]
    pub partition_key_range: KeyRange,

    /// Associated epoch metadata version
    ///
    /// This value **MUST** be set in version v1.6
    /// Optional only for backward compatibility
    ///
    /// *Since v1.6*
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag(4))]
    pub epoch_version: Option<Version>,
    /// Current replica set configuration at the time of the announcement.
    /// This field is optional for backward compatibility with older versions.
    /// *Since v1.6*
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag(5))]
    pub current_config: Option<CurrentReplicaSetConfiguration>,
    /// Next replica set configuration.
    /// *Since v1.6*
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag(6))]
    pub next_config: Option<NextReplicaSetConfiguration>,
}

bilrost_storage_encode_decode!(AnnounceLeader);

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct CurrentReplicaSetConfiguration {
    #[bilrost(tag = 1)]
    pub version: Version,
    #[bilrost(tag = 2)]
    pub replica_set: NodeSet,
    #[bilrost(tag = 3)]
    pub modified_at: MillisSinceEpoch,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[bilrost(tag = 4)]
    pub replication: ReplicationProperty,
}

impl From<PartitionConfiguration> for CurrentReplicaSetConfiguration {
    fn from(value: PartitionConfiguration) -> Self {
        Self {
            version: value.version(),
            modified_at: value.modified_at(),
            replication: value.replication().clone(),
            replica_set: value.into_replica_set(),
        }
    }
}

impl CurrentReplicaSetConfiguration {
    pub fn to_replica_set_state(&self) -> ReplicaSetState {
        new_replica_set_state(self.version, &self.replica_set)
    }

    pub fn to_current_replica_set_state(&self) -> CurrentReplicaSetState {
        CurrentReplicaSetState {
            replica_set: new_replica_set_state(self.version, &self.replica_set),
            modified_at: self.modified_at,
            replication: self.replication.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct NextReplicaSetConfiguration {
    #[bilrost(tag(1))]
    pub version: Version,
    #[bilrost(tag(2))]
    pub replica_set: NodeSet,
}

impl From<PartitionConfiguration> for NextReplicaSetConfiguration {
    fn from(value: PartitionConfiguration) -> Self {
        Self {
            version: value.version(),
            replica_set: value.into_replica_set(),
        }
    }
}

impl NextReplicaSetConfiguration {
    pub fn new(replica_set: &ReplicaSetState) -> Self {
        Self {
            version: replica_set.version,
            replica_set: replica_set.members.iter().map(|m| m.node_id).collect(),
        }
    }

    pub fn to_replica_set_state(&self) -> ReplicaSetState {
        new_replica_set_state(self.version, &self.replica_set)
    }

    pub fn to_next_replica_set_state(&self) -> NextReplicaSetState {
        NextReplicaSetState {
            replica_set: new_replica_set_state(self.version, &self.replica_set),
        }
    }
}

fn new_replica_set_state(version: Version, node_set: &NodeSet) -> ReplicaSetState {
    let members = node_set
        .iter()
        .map(|node_id| MemberState {
            node_id: *node_id,
            durable_lsn: Lsn::INVALID,
        })
        .collect();

    ReplicaSetState { version, members }
}
/// A version barrier to fence off state machine changes that require a certain minimum
/// version of restate server.
///
/// Readers before v1.4.0 will crash when reading this command. For v1.4.0+, the barrier defines the
/// minimum version of restate server that can progress after this command. It also updates the FSM
/// in case command has been trimmed.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message, serde::Serialize, serde::Deserialize)]
pub struct VersionBarrier {
    /// The minimum version required (inclusive) to progress after this barrier.
    pub version: SemanticRestateVersion,
    /// A human-readable reason for why this barrier exists.
    pub human_reason: Option<String>,
    pub partition_key_range: Keys,
}

bilrost_storage_encode_decode!(VersionBarrier);

/// Updates the `PARTITION_DURABILITY` FSM variable to the given value. Note that durability
/// only applies to partitions with the same `partition_id`. At replay time, the partition will
/// ignore updates that are not targeted to its own ID.
///
/// NOTE: The durability point is monotonically increasing.
///
/// Since v1.4.2.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message, serde::Serialize, serde::Deserialize)]
pub struct PartitionDurability {
    pub partition_id: PartitionId,
    /// The partition has applied this LSN durably to the replica-set and/or has been
    /// persisted in a snapshot in the snapshot repository.
    pub durable_point: Lsn,
    /// Timestamp which the durability point was updated
    pub modification_time: MillisSinceEpoch,
}

bilrost_storage_encode_decode!(PartitionDurability);

/// Consistently store schema across partition replicas.
///
/// Since v1.6.0.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UpsertSchema {
    pub partition_key_range: Keys,
    pub schema: Schema,
}

flexbuffers_storage_encode_decode!(UpsertSchema);
