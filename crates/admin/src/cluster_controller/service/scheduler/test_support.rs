// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Narrow test-only access to the scheduler's reconfiguration and leader-selection rules.
//!
//! This deliberately does not construct a scheduler or metadata store. The production loop owns
//! metadata persistence; deterministic integration tests only model the resulting current/next
//! update in order to explore the observable decision ordering.

use std::collections::BTreeMap;

use restate_types::PlainNodeId;
use restate_types::cluster::cluster_state::{
    LegacyClusterState, NodeState as LegacyNodeState, ReplayStatus,
};
use restate_types::cluster_state::ClusterState;
use restate_types::identifiers::PartitionId;
use restate_types::net::partition_processor_manager::ControlProcessor;
use restate_types::nodes_config::{NodesConfiguration, WorkerState};
use restate_types::partitions::PartitionConfiguration;
use restate_types::partitions::leadership_policy::LeadershipPolicy;
use restate_types::partitions::placement_policy::PlacementPolicy;

use super::{PartitionState, select_leader_by_priority, should_complete_reconfiguration};

/// The scheduler gate under test. `WaitForAddedReplica` is the exact readiness condition from
/// #5150; it is kept in test support because this branch predates that production change.
#[derive(Debug, Clone, Copy)]
pub enum ReconfigurationGate {
    Current,
    WaitForAddedReplica,
}

fn should_complete_reconfiguration_waiting_for_added_replica(
    partition_id: PartitionId,
    nodes_config: &NodesConfiguration,
    partition_state: &PartitionState,
    legacy_cluster_state: &LegacyClusterState,
) -> bool {
    let Some(next) = partition_state.next.as_ref() else {
        return false;
    };

    let all_current_workers_disabled = partition_state
        .current
        .replica_set()
        .iter()
        .all(|node_id| nodes_config.get_worker_state(node_id) == WorkerState::Disabled);
    if next.replica_set().is_empty() || all_current_workers_disabled {
        return true;
    }

    let is_added_replica_active = |node_id| {
        let Ok(node_config) = nodes_config.find_node_by_id(node_id) else {
            return false;
        };
        let Some(LegacyNodeState::Alive(node_state)) = legacy_cluster_state.nodes.get(&node_id)
        else {
            return false;
        };
        node_state.generational_node_id == node_config.current_generation
            && node_state
                .partitions
                .get(&partition_id)
                .is_some_and(|status| status.replay_status == ReplayStatus::Active)
    };
    let mut newly_added = next
        .replica_set()
        .difference(partition_state.current.replica_set());
    let Some(first_newly_added) = newly_added.next() else {
        return next.replica_set().iter().any(|node_id| {
            legacy_cluster_state.is_partition_processor_active(&partition_id, node_id)
        });
    };
    is_added_replica_active(first_newly_added) || newly_added.any(is_added_replica_active)
}

#[derive(Debug)]
pub struct PartitionEvaluation {
    pub completed_reconfiguration: bool,
    pub current: PartitionConfiguration,
    pub next: Option<PartitionConfiguration>,
    pub target_leader: Option<PlainNodeId>,
    pub commands: BTreeMap<PlainNodeId, Vec<ControlProcessor>>,
}

/// Evaluates a single scheduler pass using the production reconfiguration gate, leader priority,
/// and instruction-generation rules.
pub fn evaluate_partition(
    partition_id: PartitionId,
    current: PartitionConfiguration,
    next: Option<PartitionConfiguration>,
    cluster_state: &ClusterState,
    legacy_cluster_state: &LegacyClusterState,
    nodes_config: &NodesConfiguration,
    gate: ReconfigurationGate,
) -> PartitionEvaluation {
    let mut partition = PartitionState::new(
        current,
        next,
        LeadershipPolicy::default(),
        PlacementPolicy::default(),
    );
    let completed_reconfiguration = match gate {
        ReconfigurationGate::Current => should_complete_reconfiguration(
            partition_id,
            nodes_config,
            &partition,
            legacy_cluster_state,
        ),
        ReconfigurationGate::WaitForAddedReplica => {
            should_complete_reconfiguration_waiting_for_added_replica(
                partition_id,
                nodes_config,
                &partition,
                legacy_cluster_state,
            )
        }
    };
    if completed_reconfiguration {
        partition.current = partition
            .next
            .take()
            .expect("reconfiguration completion requires next");
    }

    partition.target_leader = select_leader_by_priority(
        &partition,
        cluster_state,
        legacy_cluster_state,
        &partition_id,
        nodes_config,
    );

    let mut commands = BTreeMap::new();
    partition.generate_instructions(&partition_id, legacy_cluster_state, &mut commands);

    PartitionEvaluation {
        completed_reconfiguration,
        current: partition.current,
        next: partition.next,
        target_leader: partition.target_leader,
        commands,
    }
}
