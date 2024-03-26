// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use crate::identifiers::{LeaderEpoch, PartitionId};
use crate::{GenerationalNodeId, Version, Versioned};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EpochMetadata {
    version: Version,
    leader_metadata: LeaderMetadata,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LeaderMetadata {
    partition_id: PartitionId,
    node_id: GenerationalNodeId,
}

impl Versioned for EpochMetadata {
    fn version(&self) -> Version {
        self.version
    }
}

impl EpochMetadata {
    pub fn new(node_id: GenerationalNodeId, partition_id: PartitionId) -> Self {
        Self {
            version: Version::MIN,
            leader_metadata: LeaderMetadata {
                node_id,
                partition_id,
            },
        }
    }

    pub fn epoch(&self) -> LeaderEpoch {
        // todo think about aligning Version and LeaderEpoch types
        let version: u32 = self.version.into();
        LeaderEpoch::from(u64::from(version))
    }

    pub fn partition_id(&self) -> PartitionId {
        self.leader_metadata.partition_id
    }

    pub fn node_id(&self) -> GenerationalNodeId {
        self.leader_metadata.node_id
    }

    pub fn claim_leadership(self, node_id: GenerationalNodeId, partition_id: PartitionId) -> Self {
        Self {
            version: self.version.next(),
            leader_metadata: LeaderMetadata {
                node_id,
                partition_id,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::epoch::EpochMetadata;
    use crate::identifiers::LeaderEpoch;
    use crate::GenerationalNodeId;

    #[test]
    fn basic_operations() {
        let node_id = GenerationalNodeId::new(1, 1);
        let other_node_id = GenerationalNodeId::new(2, 1);

        let epoch = EpochMetadata::new(node_id, 0);

        assert_eq!(epoch.epoch(), LeaderEpoch::INITIAL);
        assert_eq!(epoch.partition_id(), 0);
        assert_eq!(epoch.node_id(), node_id);

        let next_epoch = epoch.claim_leadership(other_node_id, 1);

        assert_eq!(next_epoch.epoch(), LeaderEpoch::from(2));
        assert_eq!(next_epoch.partition_id(), 1);
        assert_eq!(next_epoch.node_id(), other_node_id);
    }
}
