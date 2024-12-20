// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use xxhash_rust::xxh3::Xxh3Builder;

use restate_types::cluster::cluster_state::RunMode;
use restate_types::deprecated_cluster::cluster_state::{ClusterState, NodeState};
use restate_types::identifiers::PartitionId;
use restate_types::{GenerationalNodeId, NodeId, PlainNodeId};

/// Represents the scheduler's observed state of the cluster. The scheduler will use this
/// information and the target scheduling plan to instruct nodes to start/stop partition processors.
#[derive(Debug, Default, Clone)]
pub struct ObservedClusterState {
    pub partitions: HashMap<PartitionId, ObservedPartitionState, Xxh3Builder>,
    pub alive_nodes: HashMap<PlainNodeId, GenerationalNodeId, Xxh3Builder>,
    pub dead_nodes: HashSet<PlainNodeId, Xxh3Builder>,
    pub nodes_to_partitions: HashMap<PlainNodeId, HashSet<PartitionId, Xxh3Builder>, Xxh3Builder>,
}

impl ObservedClusterState {
    pub fn is_node_alive(&self, node_id: impl Into<NodeId>) -> bool {
        let node_id = node_id.into();

        match node_id {
            NodeId::Plain(plain_node_id) => self.alive_nodes.contains_key(&plain_node_id),
            NodeId::Generational(generational_node_id) => self
                .alive_nodes
                .get(&generational_node_id.as_plain())
                .is_some_and(|node_id| *node_id == generational_node_id),
        }
    }

    pub fn update(&mut self, cluster_state: &ClusterState) {
        self.update_nodes(cluster_state);
        self.update_partitions(cluster_state);
    }

    /// Update observed cluster state with given [`ClusterState`]
    /// Nodes in [`NodeState::Suspect`] state are treated as [`NodeState::Alive`].
    /// This means that their `last know` state will not be cleared up yet
    /// until they are marked as dead by the [`ClusterState`].
    fn update_nodes(&mut self, cluster_state: &ClusterState) {
        for (node_id, node_state) in &cluster_state.nodes {
            match node_state {
                NodeState::Alive(alive_node) => {
                    self.dead_nodes.remove(node_id);
                    self.alive_nodes
                        .insert(*node_id, alive_node.generational_node_id);
                }
                NodeState::Suspect(maybe_node) => {
                    self.dead_nodes.remove(node_id);
                    self.alive_nodes
                        .insert(*node_id, maybe_node.generational_node_id);
                }
                NodeState::Dead(_) => {
                    self.alive_nodes.remove(node_id);
                    self.dead_nodes.insert(*node_id);
                }
            }
        }
    }

    fn update_partitions(&mut self, cluster_state: &ClusterState) {
        // remove dead nodes
        for dead_node in cluster_state.dead_nodes() {
            if let Some(partitions) = self.nodes_to_partitions.remove(dead_node) {
                for partition_id in partitions {
                    if let Some(partition) = self.partitions.get_mut(&partition_id) {
                        partition.remove_partition_processor(dead_node);
                    }
                }
            }
        }

        // update node_sets and leaders of partitions
        for alive_node in cluster_state.alive_nodes() {
            let mut current_partitions = HashSet::default();

            let node_id = alive_node.generational_node_id.as_plain();

            for (partition_id, status) in &alive_node.partitions {
                let partition = self.partitions.entry(*partition_id).or_default();
                partition.upsert_partition_processor(node_id, status.effective_mode);

                current_partitions.insert(*partition_id);
            }

            if let Some(previous_partitions) = self.nodes_to_partitions.get(&node_id) {
                // remove partitions that are no longer running on the given node
                for partition_id in previous_partitions.difference(&current_partitions) {
                    if let Some(partition) = self.partitions.get_mut(partition_id) {
                        partition.remove_partition_processor(&node_id);
                    }
                }
            }

            // update nodes to partition index
            self.nodes_to_partitions.insert(node_id, current_partitions);
        }

        // remove empty partitions
        self.partitions
            .retain(|_, partition| !partition.partition_processors.is_empty());
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ObservedPartitionState {
    pub partition_processors: HashMap<PlainNodeId, RunMode, Xxh3Builder>,
}

impl ObservedPartitionState {
    fn remove_partition_processor(&mut self, node_id: &PlainNodeId) {
        self.partition_processors.remove(node_id);
    }

    fn upsert_partition_processor(&mut self, node_id: PlainNodeId, run_mode: RunMode) {
        self.partition_processors.insert(node_id, run_mode);
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster_controller::observed_cluster_state::{
        ObservedClusterState, ObservedPartitionState,
    };
    use googletest::prelude::{empty, eq};
    use googletest::{assert_that, elements_are, unordered_elements_are};
    use restate_types::cluster::cluster_state::{PartitionProcessorStatus, RunMode};
    use restate_types::deprecated_cluster::cluster_state::{
        AliveNode, ClusterState, DeadNode, NodeState,
    };
    use restate_types::identifiers::PartitionId;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};
    use std::collections::{BTreeMap, HashMap};

    impl ObservedClusterState {
        pub fn remove_node_from_partition(
            &mut self,
            partition_id: &PartitionId,
            node_id: &PlainNodeId,
        ) {
            if let Some(partition) = self.partitions.get_mut(partition_id) {
                partition.remove_partition_processor(node_id)
            }
            if let Some(partitions) = self.nodes_to_partitions.get_mut(node_id) {
                partitions.remove(partition_id);
            }
        }

        pub fn add_node_to_partition(
            &mut self,
            partition_id: PartitionId,
            node_id: PlainNodeId,
            run_mode: RunMode,
        ) {
            self.partitions
                .entry(partition_id)
                .or_default()
                .upsert_partition_processor(node_id, run_mode);
            self.nodes_to_partitions
                .entry(node_id)
                .or_default()
                .insert(partition_id);
        }
    }

    impl ObservedPartitionState {
        pub fn new(partition_processors: impl IntoIterator<Item = (PlainNodeId, RunMode)>) -> Self {
            let partition_processors: HashMap<_, _, _> = partition_processors.into_iter().collect();

            Self {
                partition_processors,
            }
        }
    }

    fn leader_partition() -> PartitionProcessorStatus {
        PartitionProcessorStatus {
            planned_mode: RunMode::Leader,
            effective_mode: RunMode::Leader,
            ..PartitionProcessorStatus::default()
        }
    }

    fn follower_partition() -> PartitionProcessorStatus {
        PartitionProcessorStatus::default()
    }

    fn alive_node(
        generational_node_id: GenerationalNodeId,
        partitions: BTreeMap<PartitionId, PartitionProcessorStatus>,
    ) -> NodeState {
        NodeState::Alive(AliveNode {
            generational_node_id,
            last_heartbeat_at: MillisSinceEpoch::now(),
            partitions,
        })
    }

    fn dead_node() -> NodeState {
        NodeState::Dead(DeadNode {
            last_seen_alive: None,
        })
    }

    #[test]
    fn observed_partition_state_updates_leader() {
        let node_1 = PlainNodeId::from(1);
        let node_2 = PlainNodeId::from(2);
        let node_3 = PlainNodeId::from(3);

        let mut state = ObservedPartitionState::default();
        assert_that!(state.partition_processors, empty());

        state.upsert_partition_processor(node_1, RunMode::Leader);
        state.upsert_partition_processor(node_2, RunMode::Leader);
        state.upsert_partition_processor(node_3, RunMode::Follower);

        assert_that!(
            state.partition_processors,
            unordered_elements_are![
                (eq(node_1), eq(RunMode::Leader)),
                (eq(node_2), eq(RunMode::Leader)),
                (eq(node_3), eq(RunMode::Follower))
            ]
        );

        state.remove_partition_processor(&node_2);

        assert_that!(
            state.partition_processors,
            unordered_elements_are![
                (eq(node_1), eq(RunMode::Leader)),
                (eq(node_3), eq(RunMode::Follower))
            ]
        );
    }

    #[test]
    fn updating_observed_cluster_state() {
        let mut observed_cluster_state = ObservedClusterState::default();
        let partition_1 = PartitionId::from(0);
        let partition_2 = PartitionId::from(1);
        let partition_3 = PartitionId::from(2);
        let node_1 = GenerationalNodeId::new(1, 0);
        let partitions_1 = [
            (partition_1, leader_partition()),
            (partition_2, leader_partition()),
        ]
        .into_iter()
        .collect();
        let node_2 = GenerationalNodeId::new(2, 0);
        let partitions_2 = [
            (partition_1, follower_partition()),
            (partition_2, follower_partition()),
        ]
        .into_iter()
        .collect();

        let cluster_state = ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes: [
                (node_1.as_plain(), alive_node(node_1, partitions_1)),
                (node_2.as_plain(), alive_node(node_2, partitions_2)),
            ]
            .into_iter()
            .collect(),
        };

        observed_cluster_state.update(&cluster_state);

        assert_that!(
            observed_cluster_state
                .alive_nodes
                .keys()
                .collect::<Vec<_>>(),
            unordered_elements_are![eq(&node_1.as_plain()), eq(&node_2.as_plain())]
        );
        assert_that!(observed_cluster_state.dead_nodes, empty());
        assert_that!(
            observed_cluster_state.nodes_to_partitions,
            unordered_elements_are![
                (
                    eq(node_1.as_plain()),
                    unordered_elements_are![eq(partition_1), eq(partition_2)]
                ),
                (
                    eq(node_2.as_plain()),
                    unordered_elements_are![eq(partition_1), eq(partition_2)]
                )
            ]
        );
        assert_that!(
            observed_cluster_state.partitions,
            unordered_elements_are![
                (
                    eq(partition_1),
                    eq(ObservedPartitionState::new([
                        (node_1.as_plain(), RunMode::Leader),
                        (node_2.as_plain(), RunMode::Follower)
                    ]))
                ),
                (
                    eq(partition_2),
                    eq(ObservedPartitionState::new([
                        (node_1.as_plain(), RunMode::Leader),
                        (node_2.as_plain(), RunMode::Follower)
                    ]))
                )
            ]
        );

        let partitions_1_new = [
            // forget partition_1
            (partition_2, leader_partition()),
            (partition_3, follower_partition()), // insert a new partition
        ]
        .into_iter()
        .collect();
        let cluster_state = ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes: [
                (node_1.as_plain(), alive_node(node_1, partitions_1_new)),
                // report node_2 as dead
                (node_2.as_plain(), dead_node()),
            ]
            .into_iter()
            .collect(),
        };

        observed_cluster_state.update(&cluster_state);

        assert_that!(
            observed_cluster_state
                .alive_nodes
                .keys()
                .collect::<Vec<_>>(),
            elements_are![eq(&node_1.as_plain())]
        );
        assert_that!(
            observed_cluster_state.dead_nodes,
            elements_are![eq(node_2.as_plain())]
        );
        assert_that!(
            observed_cluster_state.nodes_to_partitions,
            unordered_elements_are![(
                eq(node_1.as_plain()),
                unordered_elements_are![eq(partition_2), eq(partition_3)]
            )]
        );
        assert_that!(
            observed_cluster_state.partitions,
            unordered_elements_are![
                (
                    eq(partition_2),
                    eq(ObservedPartitionState::new([(
                        node_1.as_plain(),
                        RunMode::Leader
                    )]))
                ),
                (
                    eq(partition_3),
                    eq(ObservedPartitionState::new([(
                        node_1.as_plain(),
                        RunMode::Follower
                    )]))
                ),
            ]
        );
    }
}
