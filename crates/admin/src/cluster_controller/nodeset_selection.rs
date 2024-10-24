// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU8;

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
            return Err(NodeSelectionError::InsufficientWriteableNodes);
        }

        // ReplicationFactor(f+1) implies a minimum of 2f+1 nodes. At this point we are only
        // calculating the nodeset floor size, the actual size will be determined by the specific
        // strategy in use.
        assert!(
            min_copies < u8::MAX >> 1,
            "The replication factor implies a cluster size that exceeds the maximum supported size"
        );
        let optimal_fault_tolerant_nodeset_size = (min_copies as usize - 1) * 2 + 1;
        assert!(
            optimal_fault_tolerant_nodeset_size >= min_copies as usize,
            "The calculated minimum nodeset size can not be less than the replication factor"
        );

        let nodeset_target_size = match strategy {
            NodeSetSelectionStrategy::FaultTolerantAdaptive
            | NodeSetSelectionStrategy::FaultTolerantStrict => {
                optimal_fault_tolerant_nodeset_size
            }
            NodeSetSelectionStrategy::AllAvailable => candidates.len(),
            NodeSetSelectionStrategy::CappedNodesetSize(size) => size.get() as usize,
        };

        let nodeset_min_acceptable_size = match strategy {
            NodeSetSelectionStrategy::FaultTolerantAdaptive
            | NodeSetSelectionStrategy::AllAvailable
            | NodeSetSelectionStrategy::CappedNodesetSize(_) => min_copies as usize,
            NodeSetSelectionStrategy::FaultTolerantStrict => nodeset_target_size,
        };

        let nodeset = match strategy {
            NodeSetSelectionStrategy::FaultTolerantAdaptive
            | NodeSetSelectionStrategy::FaultTolerantStrict
            | NodeSetSelectionStrategy::CappedNodesetSize(_) => {
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

            // We ignore preferred nodes under AllAvailable as we will select all healthy nodes anyway
            NodeSetSelectionStrategy::AllAvailable => NodeSet::from_iter(candidates),
        };

        // todo: implement location scope-aware selection
        if nodeset.len() < nodeset_min_acceptable_size {
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

    /// Selects an optimal nodeset size based on the replication factor. The nodeset size is 2f+1,
    /// working backwards from a replication factor of f+1.
    ///
    /// This flavor is strict in that it will never propose a nodeset smaller than the inferred 2f+1
    /// size, making it a very conservative setting that is suitable for initial bootstrap but will
    /// limit availability when insufficient storage nodes are available.
    #[allow(unused)] // currently only used in tests
    FaultTolerantStrict,

    /// Selects at most the specified set of nodes, while respecting the replication factor. To
    /// tolerate failures, this size must be strictly greater than the replication factor. This
    /// strategy allows for advanced operators to explicitly control both the lower bound (via
    /// replication factor) and upper bound (via the nodeset cap) of loglet nodesets.
    #[allow(unused)] // currently only used in tests
    CappedNodesetSize(NonZeroU8),

    /// Use all available storage nodes. This will lead to over-replication and excessive resource
    /// utilisation in large clusters.
    #[allow(unused)] // currently only used in tests
    AllAvailable,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeSelectionError {
    #[error("Insufficient writeable nodes in the nodeset")]
    InsufficientWriteableNodes,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use enumset::{enum_set, EnumSet};
    use googletest::prelude::*;
    use rand::thread_rng;
    use restate_types::nodes_config::{
        LogServerConfig, NodeConfig, NodesConfiguration, Role, StorageState,
    };
    use restate_types::replicated_loglet::{LocationScope, NodeSet, ReplicationProperty};
    use restate_types::PlainNodeId;
    use xxhash_rust::xxh3::Xxh3Builder;

    use super::*;
    use crate::cluster_controller::observed_cluster_state::ObservedClusterState;

    #[test]
    #[should_panic(
        expected = "not implemented: only node-scoped replication is currently supported"
    )]
    fn test_select_log_servers_unsupported_replication_scope() {
        let nodes: Vec<PlainNodeId> = vec![1.into()];
        let replication =
            ReplicationProperty::with_scope(LocationScope::Zone, 1.try_into().unwrap());

        let mut nodes_config = NodesConfiguration::default();
        nodes_config.upsert_node(node(
            1,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::Provisioning,
        ));

        let observed_state = ObservedClusterState {
            alive_nodes: nodes
                .iter()
                .copied()
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            ..Default::default()
        };

        let strategy = NodeSetSelectionStrategy::FaultTolerantStrict;
        let preferred_nodes = NodeSet::empty();
        let rng = &mut thread_rng();
        NodeSetSelector::new(&nodes_config, &observed_state)
            .select(strategy, &replication, rng, &preferred_nodes)
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
            partitions: Default::default(), // not needed here
            alive_nodes: nodes
                .iter()
                .copied()
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: HashSet::default(),
            nodes_to_partitions: Default::default(),
        };

        let strategy = NodeSetSelectionStrategy::AllAvailable;
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

    #[test]
    fn test_select_log_servers_exceeds_min_replication_if_possible() {
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

        let observed_state = ObservedClusterState {
            partitions: Default::default(), // not needed here
            alive_nodes: nodes
                .iter()
                .copied()
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: HashSet::default(),
            nodes_to_partitions: Default::default(),
        };

        let strategy = NodeSetSelectionStrategy::AllAvailable;
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

    #[test]
    fn test_select_log_servers_single_node_cluster() {
        // Replicated loglets should work just fine in single-node clusters, with the FT strategy inferring that f=0
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
            partitions: Default::default(), // not needed here
            alive_nodes: nodes
                .iter()
                .copied()
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: HashSet::default(),
            nodes_to_partitions: Default::default(),
        };

        let strategy = NodeSetSelectionStrategy::FaultTolerantStrict;
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

    #[test]
    fn test_select_log_servers_capped_nodeset_size() {
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
        nodes_config.upsert_node(node(
            4,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        ));
        nodes_config.upsert_node(node(
            5,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        ));

        let observed_state = ObservedClusterState {
            partitions: Default::default(), // not needed here
            alive_nodes: nodes
                .iter()
                .copied()
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: HashSet::default(),
            nodes_to_partitions: Default::default(),
        };

        let strategy = NodeSetSelectionStrategy::CappedNodesetSize(3.try_into().unwrap());
        let preferred_nodes = NodeSet::empty();
        let rng = &mut thread_rng();
        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            strategy,
            &replication,
            rng,
            &preferred_nodes,
        );

        assert!(selection.is_ok());

        let nodeset = selection.unwrap();
        assert_eq!(nodeset.len(), 3);
        nodeset.iter().for_each(|node_id| {
            assert!(nodes.contains(node_id));
        });
    }

    #[test]
    fn test_select_log_servers_respects_preferred_nodes() {
        let nodes: Vec<PlainNodeId> = vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into()];
        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

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
        )); // this node has stopped responding and is considered dead
        nodes_config.upsert_node(node(
            3,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        )); // this node was previously used
        for n in 4..=10 {
            nodes_config.upsert_node(node(
                n,
                enum_set!(Role::LogServer | Role::Worker),
                StorageState::ReadWrite,
            ));
        }

        let observed_state = ObservedClusterState {
            partitions: Default::default(), // not needed here
            alive_nodes: nodes
                .iter()
                .copied()
                .filter(|id| *id != PlainNodeId::new(2))
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: HashSet::<PlainNodeId, Xxh3Builder>::from_iter([PlainNodeId::new(2)]),
            nodes_to_partitions: Default::default(),
        };

        let strategy = NodeSetSelectionStrategy::FaultTolerantStrict;
        let preferred_nodes =
            NodeSet::from_iter::<Vec<PlainNodeId>>(vec![1.into(), 2.into(), 3.into()]);
        let rng = &mut thread_rng();
        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            strategy,
            &replication,
            rng,
            &preferred_nodes,
        );

        println!("{:?}", selection);
        assert!(selection.is_ok());

        let nodeset = selection.unwrap();
        assert_that!(nodeset, contains(eq(PlainNodeId::new(3))));
        assert_that!(nodeset, not(contains(eq(PlainNodeId::new(1)))));
        assert_that!(nodeset, not(contains(eq(PlainNodeId::new(2)))));
    }

    /// In this test we have a cluster with 4 nodes, of which three were previously used for a loglet.
    /// The scenario is to verify that the nodeset selector avoids scheduling a loglet on the only two viable
    /// log servers. Although this meets the basic replication requirement, it would render the loglet vulnerable
    /// to getting stuck if a single node from its nodeset fails.
    #[test]
    fn test_select_log_servers_avoids_degraded_nodes() {
        let nodes: Vec<PlainNodeId> = vec![1.into(), 2.into(), 3.into(), 4.into()];
        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

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
        )); // this node was previously used
        nodes_config.upsert_node(node(
            4,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        )); // new node

        let mut observed_state = ObservedClusterState {
            partitions: Default::default(), // not needed here
            alive_nodes: nodes
                .iter()
                .copied()
                .filter(|id| *id != PlainNodeId::new(2))
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: HashSet::<PlainNodeId, Xxh3Builder>::from_iter([PlainNodeId::new(2)]),
            nodes_to_partitions: Default::default(),
        };

        let preferred_nodes =
            NodeSet::from_iter::<Vec<PlainNodeId>>(vec![1.into(), 2.into(), 3.into()]);
        let rng = &mut thread_rng();

        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            NodeSetSelectionStrategy::FaultTolerantStrict,
            &replication,
            rng,
            &preferred_nodes,
        );
        assert!(selection.is_err());

        // one additional healthy node should make it possible to find a viable nodeset
        nodes_config.upsert_node(node(
            5,
            enum_set!(Role::LogServer | Role::Worker),
            StorageState::ReadWrite,
        ));
        observed_state
            .alive_nodes
            .insert(5.into(), PlainNodeId::new(5).with_generation(1));
        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            NodeSetSelectionStrategy::FaultTolerantStrict,
            &replication,
            rng,
            &preferred_nodes,
        );
        assert!(selection.is_ok());
        assert_eq!(selection.unwrap().len(), 3);
    }

    /// In this test we have a cluster with 3 nodes (the nodeset selector doesn't know about sequencers).
    /// The replication factor is 2 meaning that we tolerate one failure; since this test uses the strict-ft
    /// strategy, it won't choose a new nodeset in this situation. The assumption is that the previous
    /// nodeset will continue to be used in a degraded mode.
    #[test]
    fn test_select_log_servers_respects_rf_when_availability_is_compromised() {
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
            partitions: Default::default(), // not needed here
            alive_nodes: nodes
                .iter()
                .copied()
                .map(|id| (id, id.with_generation(1)))
                .collect(),
            dead_nodes: Default::default(),
            nodes_to_partitions: Default::default(),
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

    pub fn node(
        id: impl Into<PlainNodeId>,
        roles: EnumSet<Role>,
        storage_state: StorageState,
    ) -> NodeConfig {
        let id: PlainNodeId = id.into();
        NodeConfig::new(
            format!("n{}", id),
            id.with_generation(1),
            format!("http://n{}", id).parse().unwrap(),
            roles,
            LogServerConfig { storage_state },
        )
    }
}
