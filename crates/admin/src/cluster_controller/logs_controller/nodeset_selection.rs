// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::{max, Ordering};

use itertools::Itertools;
use rand::prelude::IteratorRandom;
use rand::Rng;
use tracing::trace;

use restate_types::nodes_config::NodesConfiguration;
use restate_types::replicated_loglet::{LocationScope, NodeSet, ReplicationProperty};

use crate::cluster_controller::observed_cluster_state::ObservedClusterState;

/// Nodeset selector for picking a set of storage nodes for a replicated loglet out of a broader
/// pool of available nodes.
///
/// This selector can be reused once constructed to make multiple decisions in a single scheduling
/// iteration, if the node configuration and the replication settings are not changing.
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

    /// Determines if a nodeset can be improved by adding or replacing members. Does NOT consider
    /// sealability of the current configuration when making decisions!
    pub fn can_improve(
        &self,
        nodeset: &NodeSet,
        replication_property: &ReplicationProperty,
    ) -> bool {
        let writable_nodeset = WritableNodeSet::from(self.nodes_config);
        let alive_nodeset = writable_nodeset.alive(self.cluster_state);
        let current_alive = alive_nodeset.intersect(nodeset);

        let nodeset_size = nodeset_size_range(replication_property, writable_nodeset.len());

        if current_alive.len() == nodeset_size.target_size {
            return false;
        }

        // todo: we should check the current segment for sealability, otherwise we might propose
        //  reconfiguration when we are virtually certain to get stuck!
        alive_nodeset.len() >= nodeset_size.minimum_size
            && alive_nodeset.len() > current_alive.len()
    }

    /// Picks a set of storage nodes for a replicated loglet out of the available pool. Only alive,
    /// writable storage nodes will be used. Returns a proposed new nodeset that meets the
    /// requirements of the supplied selection strategy and replication, or an explicit error.
    pub fn select<R: Rng + ?Sized>(
        &self,
        replication_property: &ReplicationProperty,
        rng: &mut R,
        preferred_nodes: &NodeSet,
    ) -> Result<NodeSet, NodeSelectionError> {
        if replication_property.greatest_defined_scope() > LocationScope::Node {
            // todo: add support for other location scopes
            unimplemented!("only node-scoped replication is currently supported");
        }

        let writable_nodeset = WritableNodeSet::from(self.nodes_config);
        // Only consider alive, writable storage nodes.
        let alive_nodeset = writable_nodeset.alive(self.cluster_state);

        let nodeset_size = nodeset_size_range(replication_property, writable_nodeset.len());

        if writable_nodeset.len() < nodeset_size.fault_tolerant_size {
            trace!(
                nodes_count = %writable_nodeset.len(),
                ?nodeset_size.minimum_size,
                ?nodeset_size.fault_tolerant_size,
                cluster_state = ?self.cluster_state,
                nodes_config = ?self.nodes_config,
                "Not enough nodes to meet the fault tolerant replication requirements"
            );
            return Err(NodeSelectionError::InsufficientWriteableNodes);
        }

        let mut nodes = preferred_nodes
            .iter()
            .copied()
            .filter(|node_id| alive_nodeset.contains(node_id))
            .choose_multiple(rng, nodeset_size.target_size);

        if nodes.len() < nodeset_size.target_size {
            let remaining = nodeset_size.target_size - nodes.len();
            nodes.extend(
                alive_nodeset
                    .iter()
                    .filter(|node_id| !preferred_nodes.contains(node_id))
                    .choose_multiple(rng, remaining),
            );
        }

        if nodes.len() < nodeset_size.minimum_size {
            trace!(
                        "Failed to place replicated loglet: insufficient alive nodes to meet minimum size requirement {} < {}",
                        nodes.len(),
                        nodeset_size.minimum_size,
                    );

            return Err(NodeSelectionError::InsufficientWriteableNodes);
        }

        // last possibility is if the selected nodeset is still
        // smaller than fault tolerant size we try to extend from the full nodeset
        // which includes possibly dead nodes
        if nodes.len() < nodeset_size.fault_tolerant_size {
            // greedy approach: Every other node that is not
            // already in the set.
            let remaining = nodeset_size.fault_tolerant_size - nodes.len();

            let extension = writable_nodeset
                .iter()
                .filter(|node_id| !alive_nodeset.contains(node_id))
                .cloned()
                .sorted_by(|l, r| {
                    // sorting nodes by "preferred" nodes. Preferred nodes comes first.
                    match (preferred_nodes.contains(l), preferred_nodes.contains(r)) {
                        (true, true) | (false, false) => Ordering::Equal,
                        (true, false) => Ordering::Less,
                        (false, true) => Ordering::Greater,
                    }
                })
                .take(remaining);

            nodes.extend(extension);
        }

        let nodes_len = nodes.len();
        let nodeset = NodeSet::from_iter(nodes);
        assert_eq!(
            nodeset.len(),
            nodes_len,
            "We have accidentally chosen duplicate candidates during nodeset selection"
        );

        // even with all possible dead node we still can't reach the fault tolerant
        // nodeset size. This means there are not enough nodes in the cluster
        // so we still return an error.

        // todo: implement location scope-aware selection
        if nodeset.len() < nodeset_size.fault_tolerant_size {
            trace!(
                "Failed to place replicated loglet: insufficient writeable nodes to meet fault tolerant size requirement {} < {}",
                nodeset.len(),
                nodeset_size.fault_tolerant_size,
            );
            return Err(NodeSelectionError::InsufficientWriteableNodes);
        }

        Ok(nodeset)
    }
}

#[derive(Debug)]
struct NodeSetSizeRange {
    /// Minimum number of nodes required to maintain write availability;
    /// dropping below this threshold will result in loss of write availability.
    minimum_size: usize,
    /// The minimum number of nodes to satisfy replication
    /// property with fault tolerance
    ///
    /// calculated as (minimum_size - 1) * 2 + 1
    fault_tolerant_size: usize,
    /// The proposed number of nodes to use if possible
    target_size: usize,
}

fn nodeset_size_range(
    replication_property: &ReplicationProperty,
    writable_nodes_size: usize,
) -> NodeSetSizeRange {
    let min_copies = replication_property.num_copies();

    // ReplicationFactor(f+1) implies a minimum of 2f+1 nodes. At this point we are only
    // calculating the nodeset floor size, the actual size will be determined by the specific
    // strategy in use.
    assert!(
        min_copies < u8::MAX >> 1,
        "The replication factor implies a cluster size that exceeds the maximum supported size"
    );

    let fault_tolerant_size = (usize::from(min_copies) - 1) * 2 + 1;
    assert!(
        fault_tolerant_size >= usize::from(min_copies),
        "The calculated minimum nodeset size can not be less than the replication factor"
    );

    // writable_nodes_size includes any writable node (log-server) dead or alive.
    // in the greedy strategy we take the max(fault_tolerant, writable_nodes_size) as
    // our target size

    let nodeset_target_size = max(fault_tolerant_size, writable_nodes_size);

    NodeSetSizeRange {
        minimum_size: min_copies.into(),
        fault_tolerant_size,
        target_size: nodeset_target_size,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeSelectionError {
    #[error("Insufficient writeable nodes in the nodeset")]
    InsufficientWriteableNodes,
}

/// Set of all log-server nodeset, regardless of the state
#[derive(Debug, Clone, Eq, PartialEq, derive_more::Into, derive_more::Deref)]
struct WritableNodeSet(NodeSet);

impl WritableNodeSet {
    fn alive(&self, state: &ObservedClusterState) -> AliveNodeSet {
        self.iter()
            .cloned()
            .filter(|id| state.is_node_alive(*id))
            .collect::<NodeSet>()
            .into()
    }
}

impl From<&NodesConfiguration> for WritableNodeSet {
    fn from(value: &NodesConfiguration) -> Self {
        Self(
            value
                .iter()
                .filter_map(|(node_id, config)| {
                    if config.log_server_config.storage_state.can_write_to() {
                        Some(node_id)
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }
}

/// A subset of WritableNodeset that is known to be alive at the time of creation.
#[derive(Debug, Clone, Eq, PartialEq, derive_more::Into, derive_more::Deref, derive_more::From)]
struct AliveNodeSet(NodeSet);

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;

    use enumset::enum_set;
    use rand::thread_rng;

    use restate_types::nodes_config::{NodesConfiguration, Role, StorageState};
    use restate_types::replicated_loglet::{LocationScope, NodeSet, ReplicationProperty};
    use restate_types::PlainNodeId;

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
        NodeSetSelector::new(&nodes_config, &observed_state)
            .select(&replication, &mut thread_rng(), &preferred_nodes)
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

        let preferred_nodes = NodeSet::empty();
        let selection = NodeSetSelector::new(&nodes_config, &observed_state).select(
            &replication,
            &mut thread_rng(),
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

        let preferred_nodes = NodeSet::empty();
        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
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

    /// In this test we have a cluster with 3 nodes and replication factor is 2. The strict FT
    /// strategy can bootstrap a loglet using all 3 nodes but won't choose a new nodeset when only 2
    /// are alive, as that puts the loglet at risk. The assumption is that the previous nodeset will
    /// carry on in its original configuration - it is the data plane's problem to work around
    /// partial node availability. When an additional log server becomes available, the selector can
    /// reconfigure the loglet to use it.
    #[test]
    fn test_select_log_servers_respects_replication_factor() {
        let mut nodes = MockNodes::builder()
            .with_mixed_server_nodes([1, 2, 3])
            .build();

        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

        // initial selection - no prior preferences
        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            &replication,
            &mut thread_rng(),
            &NodeSet::empty(),
        );
        assert!(selection.is_ok());
        let initial_nodeset = selection.unwrap();
        assert_eq!(initial_nodeset, NodeSet::from([1, 2, 3]));

        nodes.kill_node(1);

        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            &replication,
            &mut thread_rng(),
            &initial_nodeset, // preferred nodes
        );

        // while one node is dead, the selector can still satisfy a write quorum
        // based on supplied replication property. The dead node will be included
        // in the nodeset.
        assert!(selection.is_ok());
        let initial_nodeset = selection.unwrap();
        assert_eq!(initial_nodeset, NodeSet::from([1, 2, 3]));

        nodes.add_dedicated_log_server_node(4);

        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            &replication,
            &mut thread_rng(),
            &initial_nodeset, // preferred nodes
        );
        assert_eq!(selection.unwrap(), NodeSet::from([2, 3, 4]));
    }

    #[test]
    fn test_select_log_servers_respects_replication_factor_not_enough_nodes() {
        let nodes = MockNodes::builder().with_mixed_server_nodes([1, 2]).build();

        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

        // initial selection - no prior preferences
        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            &replication,
            &mut thread_rng(),
            &NodeSet::empty(),
        );

        // in this case, the entire cluster does not have enough nodes for an optimal
        // nodeset.
        assert_eq!(
            selection,
            Err(NodeSelectionError::InsufficientWriteableNodes),
            "The strict FT strategy does not compromise on the minimum 2f+1 nodeset size"
        );
    }

    #[test]
    fn test_select_log_servers_insufficient_fault_tolerant_capacity() {
        // while we only have 2 alive node, the algorithm will still
        // prefer to use a dead node instead of failing as long as
        // we have write availability

        let nodes = MockNodes::builder()
            .with_nodes(
                [1, 2, 3],
                enum_set!(Role::LogServer | Role::Worker),
                StorageState::ReadWrite,
            )
            .dead_nodes([3])
            .build();

        let replication =
            ReplicationProperty::with_scope(LocationScope::Node, 2.try_into().unwrap());

        let preferred_nodes = NodeSet::empty();
        let selection = NodeSetSelector::new(&nodes.nodes_config, &nodes.observed_state).select(
            &replication,
            &mut thread_rng(),
            &preferred_nodes,
        );

        assert!(selection.is_ok());
        let selection = selection.unwrap();
        assert!(selection.contains(&PlainNodeId::from(3)));
    }
}
