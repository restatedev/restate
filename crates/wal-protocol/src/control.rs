// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::logs::{HasRecordKeys, Keys, Lsn};
use restate_types::schema::Schema;
use restate_types::time::MillisSinceEpoch;
use restate_types::{
    GenerationalNodeId, SemanticRestateVersion, bilrost_storage_encode_decode,
    flexbuffers_storage_encode_decode,
};

/// Announcing a new leader. This message can be written by any component to make the specified
/// partition processor the leader.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AnnounceLeader {
    /// Sender of the announce leader message.
    ///
    /// This became non-optional in v1.5. Noting that it has always been set in previous versions,
    /// it's safe to assume that it's always set.
    #[bilrost(1)]
    pub node_id: GenerationalNodeId,
    #[bilrost(2)]
    pub leader_epoch: LeaderEpoch,
    #[bilrost(3)]
    pub partition_key_range: RangeInclusive<PartitionKey>,
}

bilrost_storage_encode_decode!(AnnounceLeader);

impl HasRecordKeys for AnnounceLeader {
    fn record_keys(&self) -> Keys {
        Keys::RangeInclusive(self.partition_key_range.clone())
    }
}
/// A version barrier to fence off state machine changes that require a certain minimum
/// version of restate server.
///
/// Readers before v1.4.0 will crash when reading this command. For v1.4.0+, the barrier defines the
/// minimum version of restate server that can progress after this command. It also updates the FSM
/// in case command has been trimmed.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VersionBarrier {
    /// The minimum version required (inclusive) to progress after this barrier.
    #[bilrost(1)]
    pub version: SemanticRestateVersion,
    /// A human-readable reason for why this barrier exists.
    #[bilrost(2)]
    pub human_reason: Option<String>,
    #[bilrost(3)]
    pub partition_key_range: Keys,
}

bilrost_storage_encode_decode!(VersionBarrier);

impl HasRecordKeys for VersionBarrier {
    fn record_keys(&self) -> Keys {
        self.partition_key_range.clone()
    }
}

/// Updates the `PARTITION_DURABILITY` FSM variable to the given value. Note that durability
/// only applies to partitions with the same `partition_id`. At replay time, the partition will
/// ignore updates that are not targeted to its own ID.
///
/// NOTE: The durability point is monotonically increasing.
///
/// Since v1.4.2.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PartitionDurability {
    #[bilrost(1)]
    pub partition_id: PartitionId,
    /// The partition has applied this LSN durably to the replica-set and/or has been
    /// persisted in a snapshot in the snapshot repository.
    #[bilrost(2)]
    pub durable_point: Lsn,
    /// Timestamp which the durability point was updated
    #[bilrost(3)]
    pub modification_time: MillisSinceEpoch,
}

bilrost_storage_encode_decode!(PartitionDurability);

/// Consistently store schema across partition replicas.
///
/// Since v1.6.0.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UpsertSchema {
    pub partition_key_range: Keys,
    pub schema: Schema,
}

flexbuffers_storage_encode_decode!(UpsertSchema);

impl HasRecordKeys for UpsertSchema {
    fn record_keys(&self) -> Keys {
        self.partition_key_range.clone()
    }
}
