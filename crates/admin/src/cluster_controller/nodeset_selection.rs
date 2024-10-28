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
use std::cmp::max;
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
        // strategy in use.
        assert!(
            min_copies < u8::MAX >> 1,
            "The replication factor implies a cluster size that exceeds the maximum supported size"
        );
        let optimal_fault_tolerant_nodeset_size = (usize::from(min_copies) - 1) * 2 + 1;
        assert!(
            optimal_fault_tolerant_nodeset_size >= min_copies as usize,
            "The calculated minimum nodeset size can not be less than the replication factor"
        );

        let (nodeset_min_size, nodeset_target_size) = match strategy {
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy => (
                optimal_fault_tolerant_nodeset_size,
                max(optimal_fault_tolerant_nodeset_size, candidates.len()),
            ),
        };

        let nodeset = match strategy {
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy => {
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

/// Nodeset selection strategy for picking cluster members to host replicated logs. Note that this
/// concerns loglet replication configuration across storage servers during log bootstrap or cluster
/// reconfiguration, for example when expanding capacity.
///
/// It is expected that the Bifrost data plane will deal with short-term server unavailability.
/// Therefore, we can afford to aim high with our nodeset selections and optimise for maximum
/// possible fault tolerance. It is the data plane's responsibility to achieve availability within
/// this nodeset during periods of individual node downtime.
///
/// Finally, nodeset selection is orthogonal to log sequencer placement.
#[cfg(feature = "replicated-loglet")]
#[derive(Debug, Clone, Default)]
pub enum NodeSetSelectionStrategy {
    /// Selects an optimal nodeset size based on the replication factor. The nodeset size is at
    /// least 2f+1, calculated by working backwards from a replication factor of f+1. If there are
    /// more nodes available in the cluster, the strategy will use them.
    ///
    /// This strategy will never suggest a nodeset smaller than 2f+1, thus ensuring that there is
    /// always plenty of fault tolerance built into the loglet. This is a safe default choice.
    #[default]
    StrictFaultTolerantGreedy,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeSelectionError {
    #[error("Insufficient writeable nodes in the nodeset")]
    InsufficientWriteableNodes,
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;

    use enumset::enum_set;
    use googletest::prelude::*;
    use rand::thread_rng;
    use xxhash_rust::xxh3::Xxh3Builder;

    use restate_types::nodes_config::{NodesConfiguration, Role, StorageState};
    use restate_types::replicated_loglet::{LocationScope, NodeSet, ReplicationProperty};
    use restate_types::{GenerationalNodeId, PlainNodeId};

    use super::*;
    use crate::cluster_controller::logs_controller::tests::{node, MockNodes};
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
                NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
                &replication,
                rng,
                &preferred_nodes,
            )
            .unwrap(); // panics
    }

    #[test]
    fn test_select_log_servers_insufficient_capacity() {
        // rc=2, nss=cs=3 (f=1)
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

        let strategy = NodeSetSelectionStrategy::StrictFaultTolerantGreedy;
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
        let nodes = MockNodes::builder().with_mixed_server_nodes([1]).build();

        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 1.try_into().unwrap());

        let strategy = NodeSetSelectionStrategy::StrictFaultTolerantGreedy;
        let preferred_nodes = NodeSet::empty();
        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            strategy,
            &replication,
            &mut thread_rng(),
            &preferred_nodes,
        );

        assert_eq!(
            selection.unwrap(),
            NodeSet::from([1]),
            "A single-node cluster is possible with replication factor of 0"
        );
    }

    /// In this test we have a cluster with 3 nodes (the nodeset selector doesn't know about sequencers).
    /// The replication factor is 2 meaning that we tolerate one failure; since this test uses the strict-ft
    /// strategy, it won't choose a new nodeset in this situation. The assumption is that the previous
    /// nodeset will continue to be used in a degraded mode - it is the data plane's problem to work around
    /// partial node availability in the configuration.
    #[test]
    fn test_select_log_servers_respects_replication_factor() {
        let mut nodes = MockNodes::builder()
            .with_mixed_server_nodes([1, 2, 3])
            .build();

        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

        // initial selection - no prior preferences
        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
            &replication,
            &mut thread_rng(),
            &NodeSet::empty(),
        );
        assert!(selection.is_ok());
        let initial_nodeset = selection.unwrap();
        assert_eq!(initial_nodeset, NodeSet::from([1, 2, 3]));

        nodes.kill_node(1);

        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
            &replication,
            &mut thread_rng(),
            &initial_nodeset, // preferred nodes
        );
        assert_eq!(
            selection,
            Err(NodeSelectionError::InsufficientWriteableNodes),
            "The strict FT strategy does not compromise on the minimum 2f+1 nodeset size"
        );

        nodes.add_dedicated_log_server_node(4);

        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
            &replication,
            &mut thread_rng(),
            &initial_nodeset, // preferred nodes
        );
        assert_eq!(selection.unwrap(), NodeSet::from([2, 3, 4]));
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
        let preferred_nodes = NodeSet::from([1, 2, 3]);

        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
            &ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap()),
            &mut thread_rng(),
            &preferred_nodes,
        );

        let nodeset = selection.expect("nodeset selection succeeds");
        assert_that!(nodeset, contains(eq(PlainNodeId::new(3)))); // only node carried over
        assert_that!(nodeset, not(contains(eq(PlainNodeId::new(1)))));
        assert_that!(nodeset, not(contains(eq(PlainNodeId::new(2)))));
    }

    /// In this test we have a cluster with 5 nodes, of which three were previously used for a loglet.
    /// The scenario is to verify that the nodeset selector avoids scheduling a loglet on the only two viable
    /// log servers.
    #[test]
    fn test_select_log_servers_avoids_degraded_nodes() {
        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

        let mut nodes = MockNodes::builder()
            .with_node(
                1,
                enum_set!(Role::LogServer | Role::Worker),
                StorageState::DataLoss,
            ) // this node experienced storage corruption
            .with_node(
                2,
                enum_set!(Role::LogServer | Role::Worker),
                StorageState::ReadWrite,
            )
            .with_dead_node(
                3,
                enum_set!(Role::LogServer | Role::Worker),
                StorageState::ReadWrite,
            ) // this node is apparently dead
            .build();

        // pretend the previous configuration was [N1, N2, N3]
        let previous_nodeset = NodeSet::from([1, 2, 3]);

        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
            &replication,
            &mut thread_rng(),
            &previous_nodeset, // preferred
        );
        assert!(selection.is_err());

        // one additional healthy node is not enough for the strict FT strategy to reconfigure the log
        nodes.add_dedicated_log_server_node(4);

        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
            &replication,
            &mut thread_rng(),
            &previous_nodeset, // preferred
        );
        assert_eq!(
            selection,
            Err(NodeSelectionError::InsufficientWriteableNodes),
            "The strict FT strategy does not compromise on the minimum 2f+1 nodeset size, preferring \
            instead to keep the loglet nodeset unchanged"
        );

        // one more available storage node should result in a new fault-tolerant nodeset
        nodes.add_dedicated_log_server_node(5);

        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            NodeSetSelectionStrategy::StrictFaultTolerantGreedy,
            &replication,
            &mut thread_rng(),
            &previous_nodeset, // preferred
        );
        assert_eq!(selection.unwrap().len(), 3);
    }
}
