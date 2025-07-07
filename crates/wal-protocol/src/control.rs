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
use restate_types::logs::{Keys, Lsn};
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, SemanticRestateVersion};

/// Announcing a new leader. This message can be written by any component to make the specified
/// partition processor the leader.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AnnounceLeader {
    // todo: Remove once we no longer need to support rolling back to 1.0
    pub node_id: Option<GenerationalNodeId>,
    pub leader_epoch: LeaderEpoch,
    // Option for backwards compatibility. All future announce leader messages will have this set.
    // Fallback if this is not set to use Envelope's header's destination partition-key as a
    // single key filter.
    pub partition_key_range: Option<RangeInclusive<PartitionKey>>,
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
    pub version: SemanticRestateVersion,
    /// A human-readable reason for why this barrier exists.
    pub human_reason: Option<String>,
    pub partition_key_range: Keys,
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
    pub partition_id: PartitionId,
    /// The partition has applied this LSN durably to the replica-set and/or has been
    /// persisted in a snapshot in the snapshot repository.
    pub durable_point: Lsn,
    /// Timestamp which the durability point was updated
    pub modification_time: MillisSinceEpoch,
}

#[cfg(all(test, feature = "serde"))]
mod tests {
    use crate::control::AnnounceLeader;
    use bytes::BytesMut;
    use restate_types::identifiers::LeaderEpoch;
    use restate_types::storage::StorageCodec;
    use restate_types::{GenerationalNodeId, flexbuffers_storage_encode_decode};

    #[derive(Debug, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    struct OldAnnounceLeader {
        pub node_id: GenerationalNodeId,
        pub leader_epoch: LeaderEpoch,
    }

    flexbuffers_storage_encode_decode!(AnnounceLeader);
    flexbuffers_storage_encode_decode!(OldAnnounceLeader);

    #[test]
    fn ensure_compatibility() -> anyhow::Result<()> {
        let node_id = GenerationalNodeId::new(1, 2);
        let leader_epoch = LeaderEpoch::from(1337);

        let expected_announce_leader = AnnounceLeader {
            node_id: Some(node_id),
            leader_epoch,
            partition_key_range: Some(1..=100),
        };

        let old_announce_leader = OldAnnounceLeader {
            node_id,
            leader_epoch,
        };

        let mut buf = BytesMut::default();
        StorageCodec::encode(&old_announce_leader, &mut buf)?;

        let new_announce_leader = StorageCodec::decode::<AnnounceLeader, _>(&mut buf)?;

        assert_eq!(
            new_announce_leader.node_id,
            expected_announce_leader.node_id
        );
        assert_eq!(new_announce_leader.partition_key_range, None);
        assert_eq!(
            new_announce_leader.leader_epoch,
            expected_announce_leader.leader_epoch
        );

        buf.clear();
        StorageCodec::encode(&new_announce_leader, &mut buf)?;

        let announce_leader = StorageCodec::decode::<OldAnnounceLeader, _>(&mut buf)?;

        assert_eq!(announce_leader.node_id, old_announce_leader.node_id);
        assert_eq!(
            announce_leader.leader_epoch,
            old_announce_leader.leader_epoch
        );

        Ok(())
    }
}
