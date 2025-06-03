// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::TaskCenter;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::{GenerationalNodeId, NodeId};

/// Discover cluster nodes for a given partition based on the [`PartitionReplicaSetStates`] and the
/// [`ClusterState`].
#[derive(Clone)]
pub struct PartitionRouting {
    partition_replica_set_states: PartitionReplicaSetStates,
}

impl PartitionRouting {
    pub fn new(partition_replica_set_states: PartitionReplicaSetStates) -> Self {
        Self {
            partition_replica_set_states,
        }
    }

    /// Look up a suitable node to process requests for a given partition. Answers are authoritative
    /// though subject to propagation delays through the cluster in distributed deployments.
    /// Generally, as a consumer of routing information, your options are limited to backing off and
    /// retrying the request, or returning an error upstream when information is not available.
    ///
    /// A `None` response indicates that we have no knowledge about this partition.
    pub fn get_node_by_partition(&self, partition_id: PartitionId) -> Option<NodeId> {
        let membership = self
            .partition_replica_set_states
            .membership_state(partition_id);

        // if we know about a leader, then return it
        if membership.current_leader().current_leader != GenerationalNodeId::INVALID {
            return Some(NodeId::from(membership.current_leader().current_leader));
        }

        // otherwise, overlay the current configuration with the cluster state and take the first
        // node alive
        TaskCenter::with_current(|handle| membership.first_alive_node(handle.cluster_state()))
    }
}

#[cfg(any(test, feature = "test-util"))]
pub mod mocks {
    use crate::partitions::PartitionRouting;
    use restate_types::GenerationalNodeId;
    use restate_types::identifiers::{LeaderEpoch, PartitionId};
    use restate_types::partitions::state::{LeadershipState, PartitionReplicaSetStates};

    pub fn fixed_single_node(
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
    ) -> PartitionRouting {
        let partition_replica_set_states = PartitionReplicaSetStates::default();
        partition_replica_set_states.note_observed_leader(
            partition_id,
            LeadershipState {
                current_leader: node_id,
                current_leader_epoch: LeaderEpoch::INITIAL,
            },
        );

        PartitionRouting {
            partition_replica_set_states,
        }
    }
}
