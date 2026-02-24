// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::prelude::*;

use crate::PlainNodeId;
use crate::nodes_config::NodesConfiguration;
use crate::replication::NodeSet;

/// Extension trait for NodeSet with helpers for log-server/replicated-loglet use-cases
pub trait LogNodeSetExt {
    /// Returns true if all nodes in the nodeset are disabled
    fn all_disabled(&self, nodes_config: &NodesConfiguration) -> bool;
    /// Returns true if all nodes in the nodeset are provisioning
    fn all_provisioning(&self, nodes_config: &NodesConfiguration) -> bool;
    /// Filters out nodes that are not part of the effective nodeset (empty nodes)
    fn to_effective(&self, nodes_config: &NodesConfiguration) -> EffectiveNodeSet;
    /// Shuffles the nodes but puts our node-id at the end if it exists. In other words,
    /// `pop()` will return our node if it's in the nodeset.
    fn shuffle_for_reads(&self, my_node_id: impl Into<PlainNodeId>) -> Vec<PlainNodeId>;
}

impl LogNodeSetExt for NodeSet {
    fn all_disabled(&self, nodes_config: &NodesConfiguration) -> bool {
        self.is_empty()
            || self.iter().all(|node_id| {
                nodes_config
                    .get_log_server_storage_state(node_id)
                    .is_disabled()
            })
    }

    fn all_provisioning(&self, nodes_config: &NodesConfiguration) -> bool {
        self.is_empty()
            || self.iter().all(|node_id| {
                nodes_config
                    .get_log_server_storage_state(node_id)
                    .is_provisioning()
            })
    }

    fn to_effective(&self, nodes_config: &NodesConfiguration) -> EffectiveNodeSet {
        EffectiveNodeSet::new(self.clone(), nodes_config)
    }

    fn shuffle_for_reads(&self, my_node_id: impl Into<PlainNodeId>) -> Vec<PlainNodeId> {
        let my_node_id = my_node_id.into();
        let mut new_nodeset: Vec<_> = self.iter().cloned().collect();
        // Shuffle nodes
        new_nodeset.shuffle(&mut rand::rng());

        let has_my_node_idx = self.iter().position(|&x| x == my_node_id);

        // put my node at the end if it's there
        if let Some(idx) = has_my_node_idx {
            let len = new_nodeset.len();
            new_nodeset.swap(idx, len - 1);
        }

        new_nodeset
    }
}

#[serde_with::serde_as]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    derive_more::AsRef,
    derive_more::Deref,
    derive_more::DerefMut,
    derive_more::Display,
    derive_more::Into,
    derive_more::IntoIterator,
)]
pub struct EffectiveNodeSet(NodeSet);

impl EffectiveNodeSet {
    pub fn new(nodeset: NodeSet, nodes_config: &NodesConfiguration) -> Self {
        Self(
            nodeset
                .into_iter()
                .filter(|node_id| !nodes_config.get_log_server_storage_state(node_id).empty())
                .collect(),
        )
    }

    pub fn from_iter(
        nodeset: impl IntoIterator<Item = PlainNodeId>,
        nodes_config: &NodesConfiguration,
    ) -> Self {
        Self(
            nodeset
                .into_iter()
                .filter(|node_id| !nodes_config.get_log_server_storage_state(node_id).empty())
                .collect(),
        )
    }
}
