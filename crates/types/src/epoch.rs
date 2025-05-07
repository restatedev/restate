// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use crate::replication::{NodeSet, ReplicationProperty};
use crate::time::MillisSinceEpoch;
use crate::{GenerationalNodeId, Version, Versioned, flexbuffers_storage_encode_decode};
use ahash::HashMap;
use std::num::NonZeroU8;
use std::sync::LazyLock;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(from = "compatibility::EpochMetadataShadow")]
pub struct EpochMetadata {
    version: Version,
    leader_metadata: Option<LeaderMetadata>,
    epoch: LeaderEpoch,
    current: PartitionConfiguration,
    next: Option<PartitionConfiguration>,
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
    pub fn new(current: PartitionConfiguration, next: Option<PartitionConfiguration>) -> Self {
        Self {
            version: Version::MIN,
            leader_metadata: None,
            current,
            next,
            epoch: LeaderEpoch::INITIAL,
        }
    }

    pub fn into_inner(
        self,
    ) -> (
        Version,
        LeaderEpoch,
        PartitionConfiguration,
        Option<PartitionConfiguration>,
    ) {
        (self.version, self.epoch, self.current, self.next)
    }

    pub fn epoch(&self) -> LeaderEpoch {
        self.epoch
    }

    pub fn partition_id(&self) -> Option<PartitionId> {
        self.leader_metadata
            .as_ref()
            .map(|metadata| metadata.partition_id)
    }

    pub fn node_id(&self) -> Option<GenerationalNodeId> {
        self.leader_metadata
            .as_ref()
            .map(|metadata| metadata.node_id)
    }

    pub fn claim_leadership(self, node_id: GenerationalNodeId, partition_id: PartitionId) -> Self {
        Self {
            version: self.version.next(),
            leader_metadata: Some(LeaderMetadata {
                node_id,
                partition_id,
            }),
            current: self.current,
            next: self.next,
            epoch: self.epoch.next(),
        }
    }

    pub fn update_current_configuration(self, current: PartitionConfiguration) -> Self {
        Self {
            version: self.version.next(),
            leader_metadata: self.leader_metadata,
            current,
            next: self.next,
            epoch: self.epoch,
        }
    }

    pub fn reconfigure(self, mut next: PartitionConfiguration) -> Self {
        next.version = self
            .next
            .map(|next| next.version)
            .unwrap_or(self.current.version)
            .next();

        Self {
            version: self.version.next(),
            leader_metadata: self.leader_metadata,
            current: self.current,
            next: Some(next),
            epoch: self.epoch,
        }
    }

    pub fn complete_reconfiguration(self) -> Self {
        let next = self
            .next
            .expect("can only reconfigure if there is a next partition configuration");

        Self {
            version: self.version.next(),
            leader_metadata: self.leader_metadata,
            current: next,
            next: None,
            epoch: self.epoch,
        }
    }

    pub fn current(&self) -> &PartitionConfiguration {
        &self.current
    }

    pub fn next(&self) -> Option<&PartitionConfiguration> {
        self.next.as_ref()
    }
}

flexbuffers_storage_encode_decode!(EpochMetadata);

/// The Partition configuration contains information about which nodes run partition processors for
/// the given partition.
#[serde_with::serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionConfiguration {
    version: Version,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    replication: ReplicationProperty,
    pub replica_set: NodeSet,
    modified_at: MillisSinceEpoch,
    context: HashMap<String, String>,
}

/// Used as the returned value if no partition configuration was written in the [`EpochMetadata`].
static INVALID_PARTITION_CONFIGURATION: LazyLock<PartitionConfiguration> =
    LazyLock::new(|| PartitionConfiguration {
        version: Version::INVALID,
        modified_at: MillisSinceEpoch::now(),
        replication: ReplicationProperty::new(NonZeroU8::new(1).expect("1 to be greater than 0")),
        replica_set: NodeSet::new(),
        context: HashMap::default(),
    });

impl PartitionConfiguration {
    pub fn new(
        replication: ReplicationProperty,
        replica_set: NodeSet,
        context: HashMap<String, String>,
    ) -> Self {
        Self {
            version: Version::MIN,
            replication,
            replica_set,
            modified_at: MillisSinceEpoch::now(),
            context,
        }
    }

    /// Method to determine if the current partition processor configuration is valid or not based
    /// on its version.
    pub fn is_valid(&self) -> bool {
        self.version != Version::INVALID
    }
}

impl Versioned for PartitionConfiguration {
    fn version(&self) -> Version {
        self.version
    }
}

mod compatibility {
    use crate::Version;
    use crate::epoch::{
        EpochMetadata, INVALID_PARTITION_CONFIGURATION, LeaderMetadata, PartitionConfiguration,
    };
    use crate::identifiers::LeaderEpoch;

    #[derive(Debug, serde::Deserialize)]
    pub struct EpochMetadataShadow {
        version: Version,
        // make this field optional to allow us not having to write it in the future
        leader_metadata: Option<LeaderMetadata>,

        // those fields were added in version 1.3.3
        next_epoch: Option<LeaderEpoch>,
        current: Option<PartitionConfiguration>,
        next: Option<PartitionConfiguration>,
    }

    impl From<EpochMetadataShadow> for EpochMetadata {
        fn from(value: EpochMetadataShadow) -> Self {
            Self {
                version: value.version,
                leader_metadata: value.leader_metadata,

                epoch: value.next_epoch.unwrap_or_else(|| {
                    // in Restate < 1.3.3 we used the version as the epoch
                    let version: u32 = value.version.into();
                    LeaderEpoch::from(u64::from(version))
                }),
                current: value
                    .current
                    .unwrap_or_else(|| INVALID_PARTITION_CONFIGURATION.clone()),
                next: value.next,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::GenerationalNodeId;
    use crate::epoch::{EpochMetadata, PartitionConfiguration};
    use crate::identifiers::{LeaderEpoch, PartitionId};
    use crate::replication::ReplicationProperty;
    use std::collections::HashMap;

    #[test]
    fn basic_operations() {
        let node_id = GenerationalNodeId::new(1, 1);
        let other_node_id = GenerationalNodeId::new(2, 1);

        let epoch = EpochMetadata::new(
            PartitionConfiguration::new(
                ReplicationProperty::new_unchecked(1),
                vec![node_id.as_plain(), other_node_id.as_plain()]
                    .into_iter()
                    .collect(),
                HashMap::default(),
            ),
            None,
        );

        assert_eq!(epoch.epoch(), LeaderEpoch::INITIAL);

        let next_epoch = epoch.claim_leadership(other_node_id, PartitionId::from(1));

        assert_eq!(next_epoch.epoch(), LeaderEpoch::from(2));
        assert_eq!(next_epoch.partition_id(), Some(PartitionId::from(1)));
        assert_eq!(next_epoch.node_id(), Some(other_node_id));
    }
}
