// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::prelude::IteratorRandom;
use rand::Rng;
use tracing::trace;

use restate_types::nodes_config::NodesConfiguration;
use restate_types::replicated_loglet::{LocationScope, NodeSet, ReplicationProperty};

use crate::cluster_controller::observed_cluster_state::ObservedClusterState;

#[derive(
    Debug, Clone, Eq, PartialEq, derive_more::Display, derive_more::Into, derive_more::IntoIterator,
)]
struct WritableNodeSet(NodeSet);

impl WritableNodeSet {
    /// Constructs a new nodeset consisting of only alive, read-write storage nodes.
    pub fn from(cluster_state: &ObservedClusterState, nodes_config: &NodesConfiguration) -> Self {
        Self(
            cluster_state
                .alive_nodes
                .keys()
                .copied()
                .filter(|node_id| {
                    nodes_config
                        .get_log_server_storage_state(node_id)
                        .can_write_to()
                })
                .collect(),
        )
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Nodeset selector for picking a set of storage nodes for a replicated loglet out of a
/// broader pool of available nodes.
///
/// This selector can be reused once constructed to make multiple decision in a single scheduling iteration,
/// if the node configuration and the replication settings are not changing.
#[cfg(feature = "replicated-loglet")]
#[derive(Clone)]
pub struct NodeSetSelector<'a> {
    nodes_config: &'a NodesConfiguration,
    cluster_state: &'a ObservedClusterState,
}

impl<'a> NodeSetSelector<'a> {
    pub fn new(
        nodes_config: &'a NodesConfiguration,
        cluster_state: &'a ObservedClusterState,
    ) -> NodeSetSelector<'a> {
        Self {
            nodes_config,
            cluster_state,
        }
    }

    /// Picks a set of storage nodes for a replicated loglet out of the available pool. Only alive, writable
    /// storage nodes will be used.
    pub fn select<R: Rng + ?Sized>(
        &self,
        strategy: NodeSetSelectionStrategy,
        replication_property: &ReplicationProperty,
        rng: &mut R,
        preferred_nodes: &NodeSet,
    ) -> Result<NodeSet, NodeSelectionError> {
        if replication_property.at_greatest_scope().0 != &LocationScope::Node {
            // todo: add support for other location scopes
            unimplemented!("only node-scoped replication is currently supported");
        }

        // Only consider alive, writable storage nodes.
        let candidates = WritableNodeSet::from(self.cluster_state, self.nodes_config);

        let min_copies = replication_property.num_copies();
        if candidates.len() < min_copies.into() {
            trace!(
                candidate_nodes_count = ?candidates.len(),
                ?min_copies,
                cluster_state = ?self.cluster_state,
                nodes_config = ?self.nodes_config,
                "Not enough writeable nodes to meet the minimum replication requirements"
            );
            return Err(NodeSelectionError::InsufficientWriteableNodes);
        }

        // ReplicationFactor(f+1) implies a minimum of 2f+1 nodes. At this point we are only
        // calculating the nodeset floor size, the actual size will be determined by the specific
        // strategy in use. This is mainly to prevent math overflow during the size calculation.
        assert!(
            min_copies < u8::MAX >> 1,
            "The replication factor implies a cluster size that exceeds the maximum supported size"
        );
        let optimal_fault_tolerant_nodeset_size = (min_copies as usize - 1) * 2 + 1;
        assert!(
            optimal_fault_tolerant_nodeset_size >= min_copies as usize,
            "The calculated minimum nodeset size can not be less than the replication factor"
        );

        let (nodeset_min_size, nodeset_target_size) = match strategy {
            NodeSetSelectionStrategy::FaultTolerantAdaptive => {
                (min_copies as usize, optimal_fault_tolerant_nodeset_size)
            }
        };

        let nodeset = match strategy {
            NodeSetSelectionStrategy::FaultTolerantAdaptive => {
                let mut nodes = preferred_nodes
                    .iter()
                    .copied()
                    .filter(|node_id| candidates.0.contains(node_id))
                    .choose_multiple(rng, nodeset_target_size);

                if nodes.len() < nodeset_target_size {
                    let remaining = nodeset_target_size - nodes.len();
                    nodes.extend(
                        candidates
                            .into_iter()
                            .filter(|node_id| !preferred_nodes.contains(node_id))
                            .choose_multiple(rng, remaining),
                    );
                }

                let nodes_len = nodes.len();
                let nodeset = NodeSet::from_iter(nodes);
                assert_eq!(
                    nodeset.len(),
                    nodes_len,
                    "We have accidentally chosen duplicate candidates during nodeset selection"
                );
                nodeset
            }
        };

        // todo: implement location scope-aware selection
        if nodeset.len() < nodeset_min_size {
            trace!(
                selected_nodes_count = ?nodeset.len(),
                ?nodeset_target_size,
                "Failed to place replicated loglet: insufficient writeable nodes in the nodeset to meet availability goal"
            );
            return Err(NodeSelectionError::InsufficientWriteableNodes);
        }

        Ok(nodeset)
    }
}

#[cfg(feature = "replicated-loglet")]
#[derive(Debug, Clone, Default)]
pub enum NodeSetSelectionStrategy {
    /// Selects an optimal nodeset size based on the replication factor. The nodeset size is 2f+1,
    /// working backwards from a replication factor of f+1.
    ///
    /// In this mode we aim to reach the optimal target size, but will settle for less as long as
    /// the replication factor requirement is met. This strategy maximizes availability as long as
    /// there are sufficient available storage nodes.
    #[default]
    FaultTolerantAdaptive,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeSelectionError {
    #[error("Insufficient writeable nodes in the nodeset")]
    InsufficientWriteableNodes,
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;

    use enumset::{enum_set, EnumSet};
    use googletest::prelude::*;
    use rand::thread_rng;
    use restate_types::nodes_config::{
        LogServerConfig, NodeConfig, NodesConfiguration, Role, StorageState,
    };
    use restate_types::replicated_loglet::{LocationScope, NodeSet, ReplicationProperty};
    use restate_types::{GenerationalNodeId, PlainNodeId};
    use xxhash_rust::xxh3::Xxh3Builder;

    use super::*;
    use crate::cluster_controller::observed_cluster_state::ObservedClusterState;

    #[test]
    #[should_panic(
        expected = "not implemented: only node-scoped replication is currently supported"
    )]
    fn test_select_log_servers_rejects_unsupported_replication_scope() {
        let replication =
            ReplicationProperty::with_scope(LocationScope::Zone, 1.try_into().unwrap());

        let nodes_config = NodesConfiguration::default();
        let observed_state = ObservedClusterState::default();

        let preferred_nodes = NodeSet::empty();
        let rng = &mut thread_rng();
        NodeSetSelector::new(&nodes_config, &observed_state)
            .select(
                NodeSetSelectionStrategy::FaultTolerantAdaptive,
                &replication,
                rng,
                &preferred_nodes,
            )
            .unwrap(); // panics
    }

    #[test]
    fn test_select_log_servers_insufficient_capacity() {
        let nodes: Vec<PlainNodeId> = vec![1.into(), 2.into(), 3.into()];
        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

        let mut nodes_config = NodesConfiguration::default();
        nodes_config.upsert_node(node(0, enum_set!(Role::Admin), StorageState::Disabled));
        nodes_config.upsert_node(node(
            1,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::Provisioning,
        ));
        nodes_config.upsert_node(node(
            2,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        ));
        nodes_config.upsert_node(node(
            3,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadOnly,
        ));
        nodes_config.upsert_node(node(4, enum_set!(Role::Worker), StorageState::Disabled));

        let observed_state = ObservedClusterState {
            alive_nodes: nodes
                .iter()
                .copied()
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: HashSet::default(),
            ..Default::default()
        };

        let strategy = NodeSetSelectionStrategy::FaultTolerantAdaptive;
        let preferred_nodes = NodeSet::empty();
        let rng = &mut thread_rng();
        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            strategy,
            &replication,
            rng,
            &preferred_nodes,
        );

        assert_eq!(
            selection,
            Err(NodeSelectionError::InsufficientWriteableNodes)
        );
    }

    /// Replicated loglets should work just fine in single-node clusters, with the FT strategy inferring that f=0,
    /// as long as the replication factor is set to 1.
    #[test]
    fn test_select_log_servers_single_node_cluster() {
        let nodes: Vec<PlainNodeId> = vec![1.into()];
        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 1.try_into().unwrap());

        let mut nodes_config = NodesConfiguration::default();
        nodes_config.upsert_node(node(
            1,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        ));

        let observed_state = ObservedClusterState {
            alive_nodes: nodes
                .iter()
                .copied()
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: HashSet::default(),
            ..Default::default()
        };

        let strategy = NodeSetSelectionStrategy::FaultTolerantAdaptive;
        let preferred_nodes = NodeSet::empty();
        let rng = &mut thread_rng();
        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            strategy,
            &replication,
            rng,
            &preferred_nodes,
        );

        assert!(selection.is_ok());
        assert_eq!(selection.unwrap(), NodeSet::from_iter(nodes));
    }

    /// In this test we have a cluster with 3 nodes (the nodeset selector doesn't know about sequencers).
    /// The replication factor is 2 meaning that we tolerate one failure; since this test uses the strict-ft
    /// strategy, it won't choose a new nodeset in this situation. The assumption is that the previous
    /// nodeset will continue to be used in a degraded mode.
    #[test]
    fn test_select_log_servers_respects_replication_factor() {
        let nodes: Vec<PlainNodeId> = vec![1.into(), 2.into(), 3.into()];
        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

        let mut nodes_config = NodesConfiguration::default();
        nodes_config.upsert_node(node(
            1,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        ));
        nodes_config.upsert_node(node(
            2,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        ));
        nodes_config.upsert_node(node(
            3,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        ));

        let mut observed_state = ObservedClusterState {
            alive_nodes: nodes
                .iter()
                .copied()
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: Default::default(),
            ..Default::default()
        };

        let rng = &mut thread_rng();

        // initial selection - no prior preferences
        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            NodeSetSelectionStrategy::FaultTolerantAdaptive,
            &replication,
            rng,
            &NodeSet::empty(),
        );
        assert!(selection.is_ok());
        let nodeset = selection.unwrap();
        assert_eq!(nodeset.len(), 3);

        // If the previous configuration was [N1*, N2, N3] the preference is to continue using the same set
        let preferred_nodes = &nodeset;
        observed_state.alive_nodes.remove(&1.into());
        observed_state.dead_nodes.insert(1.into());

        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            NodeSetSelectionStrategy::FaultTolerantAdaptive,
            &replication,
            rng,
            preferred_nodes,
        );
        assert!(selection.is_ok());
        let nodeset = selection.unwrap();
        assert_eq!(nodeset.len(), 2); // suboptimal but still meets RF requirement!

        // Preference is to stick to [N2*, N3] from before
        let preferred_nodes = &nodeset;
        observed_state.alive_nodes.remove(&2.into());
        observed_state.dead_nodes.insert(2.into());

        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            NodeSetSelectionStrategy::FaultTolerantAdaptive,
            &replication,
            rng,
            preferred_nodes,
        );
        assert!(selection.is_err()); // we can no longer meet the RF requirement
    }

    #[test]
    fn test_select_log_servers_respects_preferred_nodes() {
        let mut nodes_config = NodesConfiguration::default();

        // previous nodeset - will be the preferred input to selection
        nodes_config.upsert_node(node(
            1,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::DataLoss,
        )); // this node is alive, but not writable
        nodes_config.upsert_node(node(
            2,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        )); // this node has stopped responding and is considered dead
        nodes_config.upsert_node(node(
            3,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        )); // this node was previously used and is still ok

        // additional available nodes to choose from
        for n in 4..=7 {
            nodes_config.upsert_node(node(
                n,
                enum_set!(Role::LogServer | Role::Worker),
                StorageState::ReadWrite,
            ));
        }

        let observed_state = ObservedClusterState {
            alive_nodes: (1..=7)
                .filter(|id| *id != 2) // N2 is dead
                .map(|id| (PlainNodeId::new(id), GenerationalNodeId::new(id, 1)))
                .collect(),
            dead_nodes: HashSet::<PlainNodeId, Xxh3Builder>::from_iter([PlainNodeId::new(2)]),
            ..Default::default()
        };
        let preferred_nodes =
            NodeSet::from_iter::<Vec<PlainNodeId>>(vec![1.into(), 2.into(), 3.into()]);

        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            NodeSetSelectionStrategy::FaultTolerantAdaptive,
            &ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap()),
            &mut thread_rng(),
            &preferred_nodes,
        );

        let nodeset = selection.expect("nodeset selection succeeds");
        assert_that!(nodeset, contains(eq(PlainNodeId::new(3)))); // only node carried over
        assert_that!(nodeset, not(contains(eq(PlainNodeId::new(1)))));
        assert_that!(nodeset, not(contains(eq(PlainNodeId::new(2)))));
    }

    /// In this test we have a cluster with 4 nodes, of which three were previously used for a loglet.
    /// The scenario is to verify that the nodeset selector avoids scheduling a loglet on the only two viable
    /// log servers.
    #[test]
    fn test_select_log_servers_avoids_degraded_nodes() {
        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());
        let nodes: Vec<PlainNodeId> = vec![1.into(), 2.into(), 3.into(), 4.into()];

        let mut nodes_config = NodesConfiguration::default();
        nodes_config.upsert_node(node(
            1,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::DataLoss,
        )); // this node is alive, but not writable
        nodes_config.upsert_node(node(
            2,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        )); // this node is in the correct storage state but has stopped responding and is considered dead (see below)
        nodes_config.upsert_node(node(
            3,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        )); // this node is still usable

        let mut observed_state = ObservedClusterState {
            alive_nodes: nodes
                .iter()
                .copied()
                .filter(|id| *id != PlainNodeId::new(2))
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: HashSet::<PlainNodeId, Xxh3Builder>::from_iter([PlainNodeId::new(2)]),
            ..Default::default()
        };

        // previously the loglet was running on [N1, N2, N3] so these are preferred
        let preferred_nodes =
            NodeSet::from_iter::<Vec<PlainNodeId>>(vec![1.into(), 2.into(), 3.into()]);
        let rng = &mut thread_rng();

        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            NodeSetSelectionStrategy::FaultTolerantAdaptive,
            &replication,
            rng,
            &preferred_nodes,
        );
        assert!(selection.is_err());

        // one additional healthy node should make it possible to find a viable nodeset
        nodes_config.upsert_node(node(
            4,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        ));
        observed_state
            .alive_nodes
            .insert(4.into(), PlainNodeId::new(4).with_generation(1));
        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            NodeSetSelectionStrategy::FaultTolerantAdaptive,
            &replication,
            rng,
            &preferred_nodes,
        );
        assert!(selection.is_ok());
        let nodeset = selection.unwrap();
        assert_eq!(nodeset.len(), 2); // meets replication requirement exactly

        // one more available storage node should result in a fault-tolerant new nodeset selection
        let preferred_nodes = &nodeset;
        nodes_config.upsert_node(node(
            5,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        ));
        observed_state
            .alive_nodes
            .insert(5.into(), PlainNodeId::new(5).with_generation(1));
        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            NodeSetSelectionStrategy::FaultTolerantAdaptive,
            &replication,
            rng,
            preferred_nodes,
        );
        assert!(selection.is_ok());
        assert_eq!(selection.unwrap().len(), 3); // now exceeds replication requirement
    }

    pub fn node(id: u32, roles: EnumSet<Role>, storage_state: StorageState) -> NodeConfig {
        NodeConfig::new(
            format!("node-{}", id),
            PlainNodeId::from(id).with_generation(1),
            format!("https://node-{}", id).parse().unwrap(),
            roles,
            LogServerConfig { storage_state },
        )
    }
}