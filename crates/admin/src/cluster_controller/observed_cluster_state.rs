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

use restate_types::cluster::cluster_state::{ClusterState, ReplayStatus, RunMode};
use restate_types::identifiers::PartitionId;
use restate_types::{GenerationalNodeId, NodeId, PlainNodeId};

/// Represents the scheduler's observed state of the cluster. The scheduler will use this
/// information and the target scheduling plan to instruct nodes to start/stop partition processors.
#[derive(Debug, Default, Clone)]
pub struct ObservedClusterState {
    pub partitions: HashMap<PartitionId, ObservedPartitionState>,
    // The logic that uses ObservedClusterState assumes that it has a snapshot of the current alive
    // nodes. We could use restate_core::cluster_state::ClusterState directly, if we kept the read
    // lock. However, since ObservedClusterState is held across await points, this would prevent
    // any concurrent updates to the ClusterState. Therefore, we keep a separate copy of the alive
    // nodes here until the users of ObservedClusterState no longer require a snapshot.
    alive_nodes: HashMap<PlainNodeId, GenerationalNodeId>,
    nodes_to_partitions: HashMap<PlainNodeId, HashSet<PartitionId>>,
}

impl ObservedClusterState {
    pub fn partition_state(&self, partition_id: &PartitionId) -> Option<&ObservedPartitionState> {
        self.partitions.get(partition_id)
    }

    pub fn alive_nodes(&self) -> impl Iterator<Item = &GenerationalNodeId> {
        self.alive_nodes.values()
    }

    pub fn alive_nodes_empty(&self) -> bool {
        self.alive_nodes.is_empty()
    }

    /// Returns the generational node id if the given node_id is alive. If a plain node id is
    /// provided, then the method returns the currently alive generational node id. If a
    /// generational node id is provided, then it will return this id if it is alive.
    pub fn alive_generation(&self, node_id: impl Into<NodeId>) -> Option<GenerationalNodeId> {
        let node_id = node_id.into();

        match node_id {
            NodeId::Plain(plain_node_id) => self.alive_nodes.get(&plain_node_id).cloned(),
            NodeId::Generational(generational_node_id) => self
                .alive_nodes
                .get(&generational_node_id.as_plain())
                .filter(|node_id| **node_id == generational_node_id)
                .copied(),
        }
    }

    pub fn update_liveness(&mut self, cs: &restate_core::cluster_state::ClusterState) {
        let mut unknown_nodes: HashSet<_> = self.nodes_to_partitions.keys().cloned().collect();

        for (node_id, state) in cs.all() {
            if state.is_alive() {
                self.alive_nodes.insert(node_id.as_plain(), node_id);
            } else {
                // remove dead nodes from other indices
                self.remove_node(&node_id.as_plain());
            }

            unknown_nodes.remove(&node_id.as_plain());
        }

        for unknown_node in unknown_nodes {
            self.remove_node(&unknown_node);
        }
    }

    pub fn remove_node(&mut self, node_id: &PlainNodeId) -> Option<GenerationalNodeId> {
        if let Some(partitions) = self.nodes_to_partitions.remove(node_id) {
            for partition in partitions {
                if let Some(observed_partition) = self.partitions.get_mut(&partition) {
                    observed_partition.remove_partition_processor(node_id);
                }
            }
        }

        self.alive_nodes.remove(node_id)
    }

    pub fn update_partitions(&mut self, cluster_state: &ClusterState) {
        // update node_sets and leaders of partitions
        for alive_node in cluster_state.alive_nodes() {
            if !self
                .alive_nodes
                .contains_key(&alive_node.generational_node_id.as_plain())
            {
                // ignore nodes that are not alive according to the cluster state
                continue;
            }

            let mut current_partitions = HashSet::default();

            let node_id = alive_node.generational_node_id.as_plain();

            for (partition_id, status) in &alive_node.partitions {
                let partition = self.partitions.entry(*partition_id).or_default();
                partition.upsert_partition_processor(
                    node_id,
                    status.effective_mode,
                    status.replay_status,
                );

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

    /// Checks whether there exists a partition processor on the given node for the given partition
    /// which is active (wrt [`ReplayStatus`]).
    pub fn is_partition_processor_active(
        &self,
        partition_id: &PartitionId,
        node_id: &PlainNodeId,
    ) -> bool {
        self.partitions
            .get(partition_id)
            .and_then(|partition| partition.partition_processors.get(node_id))
            .map(|processor_state| processor_state.replay_status == ReplayStatus::Active)
            .unwrap_or(false)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ObservedPartitionState {
    pub partition_processors: HashMap<PlainNodeId, PartitionProcessorState>,
}

impl ObservedPartitionState {
    fn remove_partition_processor(&mut self, node_id: &PlainNodeId) {
        self.partition_processors.remove(node_id);
    }

    fn upsert_partition_processor(
        &mut self,
        node_id: PlainNodeId,
        run_mode: RunMode,
        replay_status: ReplayStatus,
    ) {
        self.partition_processors.insert(
            node_id,
            PartitionProcessorState {
                run_mode,
                replay_status,
            },
        );
    }

    pub fn replay_status(&self, node_id: &PlainNodeId) -> Option<ReplayStatus> {
        self.partition_processors
            .get(node_id)
            .map(|p| p.replay_status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProcessorState {
    pub run_mode: RunMode,
    pub replay_status: ReplayStatus,
}

impl PartitionProcessorState {
    pub fn new(run_mode: RunMode, replay_status: ReplayStatus) -> Self {
        Self {
            run_mode,
            replay_status,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::cluster_controller::observed_cluster_state::{
        ObservedClusterState, ObservedPartitionState, PartitionProcessorState,
    };
    use googletest::prelude::{empty, eq};
    use googletest::{assert_that, elements_are, unordered_elements_are};
    use restate_types::cluster::cluster_state::{
        AliveNode, ClusterState, DeadNode, NodeState, PartitionProcessorStatus, ReplayStatus,
        RunMode,
    };
    use restate_types::identifiers::PartitionId;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    impl ObservedClusterState {
        pub fn add_alive_node(&mut self, node_id: GenerationalNodeId) {
            self.alive_nodes.insert(node_id.as_plain(), node_id);
        }
    }

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
            replay_status: ReplayStatus,
        ) {
            self.partitions
                .entry(partition_id)
                .or_default()
                .upsert_partition_processor(node_id, run_mode, replay_status);
            self.nodes_to_partitions
                .entry(node_id)
                .or_default()
                .insert(partition_id);
        }
    }

    impl ObservedPartitionState {
        pub fn new(
            partition_processors: impl IntoIterator<Item = (PlainNodeId, PartitionProcessorState)>,
        ) -> Self {
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
            replay_status: ReplayStatus::Active,
            ..PartitionProcessorStatus::default()
        }
    }

    fn follower_partition() -> PartitionProcessorStatus {
        PartitionProcessorStatus {
            replay_status: ReplayStatus::Active,
            ..PartitionProcessorStatus::default()
        }
    }

    fn alive_node(
        generational_node_id: GenerationalNodeId,
        partitions: BTreeMap<PartitionId, PartitionProcessorStatus>,
    ) -> NodeState {
        NodeState::Alive(AliveNode {
            generational_node_id,
            last_heartbeat_at: MillisSinceEpoch::now(),
            partitions,
            uptime: Duration::default(),
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

        state.upsert_partition_processor(node_1, RunMode::Leader, ReplayStatus::Active);
        state.upsert_partition_processor(node_2, RunMode::Leader, ReplayStatus::Active);
        state.upsert_partition_processor(node_3, RunMode::Follower, ReplayStatus::Active);

        assert_that!(
            state.partition_processors,
            unordered_elements_are![
                (
                    eq(node_1),
                    eq(PartitionProcessorState::new(
                        RunMode::Leader,
                        ReplayStatus::Active
                    ))
                ),
                (
                    eq(node_2),
                    eq(PartitionProcessorState::new(
                        RunMode::Leader,
                        ReplayStatus::Active
                    ))
                ),
                (
                    eq(node_3),
                    eq(PartitionProcessorState::new(
                        RunMode::Follower,
                        ReplayStatus::Active
                    ))
                )
            ]
        );

        state.remove_partition_processor(&node_2);

        assert_that!(
            state.partition_processors,
            unordered_elements_are![
                (
                    eq(node_1),
                    eq(PartitionProcessorState::new(
                        RunMode::Leader,
                        ReplayStatus::Active
                    ))
                ),
                (
                    eq(node_3),
                    eq(PartitionProcessorState::new(
                        RunMode::Follower,
                        ReplayStatus::Active
                    ))
                )
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

        let cs = restate_core::cluster_state::ClusterState::default();
        let mut updater = cs.clone().updater();
        updater
            .write()
            .upsert_node_state(node_1, restate_core::cluster_state::NodeState::Alive);
        updater
            .write()
            .upsert_node_state(node_2, restate_core::cluster_state::NodeState::Alive);

        observed_cluster_state.update_liveness(&cs);
        observed_cluster_state.update_partitions(&cluster_state);

        assert_that!(
            observed_cluster_state
                .alive_nodes
                .keys()
                .collect::<Vec<_>>(),
            unordered_elements_are![eq(&node_1.as_plain()), eq(&node_2.as_plain())]
        );
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
                        (
                            node_1.as_plain(),
                            PartitionProcessorState::new(RunMode::Leader, ReplayStatus::Active)
                        ),
                        (
                            node_2.as_plain(),
                            PartitionProcessorState::new(RunMode::Follower, ReplayStatus::Active)
                        )
                    ]))
                ),
                (
                    eq(partition_2),
                    eq(ObservedPartitionState::new([
                        (
                            node_1.as_plain(),
                            PartitionProcessorState::new(RunMode::Leader, ReplayStatus::Active)
                        ),
                        (
                            node_2.as_plain(),
                            PartitionProcessorState::new(RunMode::Follower, ReplayStatus::Active)
                        )
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

        updater.write().remove_node(node_2.as_plain());
        observed_cluster_state.update_liveness(&cs);
        observed_cluster_state.update_partitions(&cluster_state);

        assert_that!(
            observed_cluster_state
                .alive_nodes
                .keys()
                .collect::<Vec<_>>(),
            elements_are![eq(&node_1.as_plain())]
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
                        PartitionProcessorState::new(RunMode::Leader, ReplayStatus::Active)
                    )]))
                ),
                (
                    eq(partition_3),
                    eq(ObservedPartitionState::new([(
                        node_1.as_plain(),
                        PartitionProcessorState::new(RunMode::Follower, ReplayStatus::Active)
                    )]))
                ),
            ]
        );
    }
}
