// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use restate_types::identifiers::{LeaderEpoch, PartitionKey};
use restate_types::GenerationalNodeId;

/// Announcing a new leader. This message can be written by any component to make the specified
/// partition processor the leader.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AnnounceLeader {
    // todo: Remove once we no longer need to support rolling back to 1.0
    pub node_id: Option<GenerationalNodeId>,
    pub leader_epoch: LeaderEpoch,
    // Option for backwards compatibility. All future announce leade messages will have this set.
    // Fallback if this is not set is to use Envelope's header's destination partition-key as a
    // single key filter.
    pub partition_key_range: Option<RangeInclusive<PartitionKey>>,
}

#[cfg(test)]
mod tests {
    use crate::control::AnnounceLeader;
    use bytes::BytesMut;
    use restate_types::identifiers::LeaderEpoch;
    use restate_types::storage::StorageCodec;
    use restate_types::{flexbuffers_storage_encode_decode, GenerationalNodeId};

    #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
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
