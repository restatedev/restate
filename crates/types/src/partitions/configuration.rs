// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ahash::HashMap;

use crate::replication::{NodeSet, ReplicationProperty};
use crate::time::MillisSinceEpoch;
use crate::{Version, Versioned};

use super::state::ReplicaSetState;

/// The Partition configuration contains information about which nodes run partition processors for
/// the given partition.
#[serde_with::serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionConfiguration {
    pub(crate) version: Version,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    replication: ReplicationProperty,
    replica_set: NodeSet,
    modified_at: MillisSinceEpoch,
    context: HashMap<String, String>,
}

impl Default for PartitionConfiguration {
    fn default() -> Self {
        Self {
            version: Version::INVALID,
            replication: ReplicationProperty::new_unchecked(1),
            replica_set: NodeSet::default(),
            modified_at: MillisSinceEpoch::now(),
            context: HashMap::default(),
        }
    }
}

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

    pub fn to_replica_set_state(&self) -> ReplicaSetState {
        ReplicaSetState::from_partition_configuration(self)
    }

    pub fn replica_set(&self) -> &NodeSet {
        &self.replica_set
    }

    pub fn into_replica_set(self) -> NodeSet {
        self.replica_set
    }

    pub fn replication(&self) -> &ReplicationProperty {
        &self.replication
    }

    pub fn context(&self) -> &HashMap<String, String> {
        &self.context
    }

    pub fn modified_at(&self) -> MillisSinceEpoch {
        self.modified_at
    }

    /// Determine if the current partition configuration is valid or not
    pub fn is_valid(&self) -> bool {
        self.version != Version::INVALID
    }
}

impl Versioned for PartitionConfiguration {
    fn version(&self) -> Version {
        self.version
    }
}
