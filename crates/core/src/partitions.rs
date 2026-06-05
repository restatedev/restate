// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::GenerationalNodeId;
use restate_types::identifiers::{LeaderEpoch, PartitionId};
use restate_types::partitions::state::{LeadershipState, PartitionReplicaSetStates};

use crate::task_center;

/// Discover cluster nodes for a given partition based on the [`PartitionReplicaSetStates`] and the
/// [`ClusterState`].
#[derive(Clone)]
pub struct PartitionRouting {
    task_center: task_center::Handle,
    partition_replica_set_states: PartitionReplicaSetStates,
}

impl PartitionRouting {
    pub fn new(
        partition_replica_set_states: PartitionReplicaSetStates,
        task_center: task_center::Handle,
    ) -> Self {
        Self {
            task_center,
            partition_replica_set_states,
        }
    }

    /// Look up a suitable node to process requests for a given partition. Answers are authoritative
    /// though subject to propagation delays through the cluster in distributed deployments.
    /// Generally, as a consumer of routing information, your options are limited to backing off and
    /// retrying the request, or returning an error upstream when information is not available.
    ///
    /// A `None` response indicates that we have no knowledge about this partition.
    pub fn get_node_by_partition(&self, partition_id: PartitionId) -> Option<GenerationalNodeId> {
        let membership = self
            .partition_replica_set_states
            .membership_state(partition_id);

        // if we know about a leader, then return it
        if membership.current_leader().current_leader != GenerationalNodeId::INVALID {
            return Some(membership.current_leader().current_leader);
        }

        // otherwise, overlay the current configuration with the cluster state and take the first
        // node alive
        membership.first_alive_node(self.task_center.cluster_state())
    }

    /// The currently observed leader epoch for a given partition, subject to propagation delays
    /// through the cluster in distributed deployments.
    ///
    /// A `None` response indicates that we have no knowledge about a leader for this partition.
    pub fn get_leader_epoch(&self, partition_id: PartitionId) -> Option<LeaderEpoch> {
        let leadership_state = self
            .partition_replica_set_states
            .membership_state(partition_id)
            .current_leader();

        if leadership_state.current_leader_epoch == LeaderEpoch::INVALID {
            return None;
        }

        Some(leadership_state.current_leader_epoch)
    }

    /// The currently observed leader node and its leader epoch for a given partition, subject to
    /// propagation delays through the cluster in distributed deployments.
    ///
    /// A `None` response indicates that we have no knowledge about a leader for this partition,
    /// or that we know the leader epoch but not which node holds it.
    pub fn get_leader(
        &self,
        partition_id: PartitionId,
    ) -> Option<(GenerationalNodeId, LeaderEpoch)> {
        let leadership_state = self
            .partition_replica_set_states
            .membership_state(partition_id)
            .current_leader();

        if leadership_state.current_leader_epoch == LeaderEpoch::INVALID
            || !leadership_state.current_leader.is_valid()
        {
            return None;
        }

        Some((
            leadership_state.current_leader,
            leadership_state.current_leader_epoch,
        ))
    }

    /// Waits until a leader epoch of at least `min_epoch` has been observed for the given
    /// partition, and returns the corresponding leadership state.
    ///
    /// Note that the leadership state is propagated through the cluster asynchronously, so this
    /// may wait longer than it takes the actual leader to win the election. This future never
    /// completes if no such epoch is observed; callers should apply their own timeout or
    /// cancellation.
    pub async fn wait_for_leader_epoch(
        &self,
        partition_id: PartitionId,
        min_epoch: LeaderEpoch,
    ) -> LeadershipState {
        let mut watch = self
            .partition_replica_set_states
            .watch_leadership_state(partition_id);
        let state = watch
            .wait_for(|leadership_state| leadership_state.current_leader_epoch >= min_epoch)
            .await
            .expect("partition replica set states are never dropped while in use");
        *state
    }

    pub fn partition_replica_set_state(&self) -> &PartitionReplicaSetStates {
        &self.partition_replica_set_states
    }
}
