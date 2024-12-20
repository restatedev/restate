// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt::Display;

use super::ReplicationProperty;
use crate::logs::LogletId;
use crate::nodes_config::NodesConfiguration;
use crate::{GenerationalNodeId, PlainNodeId};
use itertools::Itertools;
use rand::seq::SliceRandom;
use serde_with::DisplayFromStr;
use xxhash_rust::xxh3::Xxh3Builder;

/// Configuration parameters of a replicated loglet segment
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct ReplicatedLogletParams {
    /// Unique identifier for this loglet
    pub loglet_id: LogletId,
    /// The sequencer node
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub sequencer: GenerationalNodeId,
    /// Replication properties of this loglet
    pub replication: ReplicationProperty,
    pub nodeset: NodeSet,
}

impl ReplicatedLogletParams {
    pub fn deserialize_from(slice: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(slice)
    }

    pub fn serialize(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[serde_with::serde_as]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Default,
    Eq,
    PartialEq,
    derive_more::IntoIterator,
    derive_more::From,
)]
pub struct NodeSet(#[serde_as(as = "HashSet<DisplayFromStr>")] HashSet<PlainNodeId>);

impl NodeSet {
    pub fn empty() -> Self {
        Self(HashSet::new())
    }

    pub fn from_single(node: PlainNodeId) -> Self {
        let mut set = HashSet::new();
        set.insert(node);
        Self(set)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn contains(&self, node: &PlainNodeId) -> bool {
        self.0.contains(node)
    }

    /// Returns true if this node didn't already exist in the nodeset
    pub fn insert(&mut self, node: PlainNodeId) -> bool {
        self.0.insert(node)
    }

    /// Returns true if all nodes in the nodeset are disabled
    pub fn all_disabled(&self, nodes_config: &NodesConfiguration) -> bool {
        self.is_empty()
            || self
                .0
                .iter()
                .map(|node_id| nodes_config.get_log_server_storage_state(node_id))
                .all(|storage_state| storage_state.is_disabled())
    }

    pub fn all_provisioning(&self, nodes_config: &NodesConfiguration) -> bool {
        self.is_empty()
            || self
                .0
                .iter()
                .map(|node_id| nodes_config.get_log_server_storage_state(node_id))
                .all(|storage_state| storage_state.is_provisioning())
    }

    /// Clears the nodeset, removes all nodes.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn remove(&mut self, node: &PlainNodeId) -> bool {
        self.0.remove(node)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &PlainNodeId> {
        self.0.iter()
    }

    /// Filters out nodes that are not part of the effective nodeset (empty nodes)
    pub fn to_effective(&self, nodes_config: &NodesConfiguration) -> EffectiveNodeSet {
        EffectiveNodeSet::new(self, nodes_config)
    }

    /// Creates a new nodeset that excludes the nodes not in the provided set
    pub fn new_excluding(&self, excluded_node_ids: &HashSet<PlainNodeId, Xxh3Builder>) -> NodeSet {
        NodeSet::from(
            self.iter()
                .copied()
                .filter(|node_id| !excluded_node_ids.contains(node_id))
                .collect::<HashSet<_>>(),
        )
    }

    pub fn intersect(&self, intersect_node_ids: &NodeSet) -> NodeSet {
        NodeSet::from(
            self.iter()
                .copied()
                .filter(|node_id| intersect_node_ids.contains(node_id))
                .collect::<HashSet<_>>(),
        )
    }

    /// Shuffles the nodes but puts our node-id at the end if it exists. In other words,
    /// `pop()` will return our node if it's in the nodeset.
    pub fn shuffle_for_reads(
        self: &NodeSet,
        my_node_id: impl Into<PlainNodeId>,
    ) -> Vec<PlainNodeId> {
        let my_node_id = my_node_id.into();
        let mut new_nodeset: Vec<_> = self.iter().cloned().collect();
        // Shuffle nodes
        new_nodeset.shuffle(&mut rand::thread_rng());

        let has_my_node_idx = self.iter().position(|&x| x == my_node_id);

        // put my node at the end if it's there
        if let Some(idx) = has_my_node_idx {
            let len = new_nodeset.len();
            new_nodeset.swap(idx, len - 1);
        }

        new_nodeset
    }
}

impl<'a> IntoIterator for &'a NodeSet {
    type Item = &'a PlainNodeId;

    type IntoIter = <&'a HashSet<PlainNodeId> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<const N: usize> From<[PlainNodeId; N]> for NodeSet {
    fn from(value: [PlainNodeId; N]) -> Self {
        Self(From::from(value))
    }
}

impl<const N: usize> From<[u32; N]> for NodeSet {
    fn from(value: [u32; N]) -> Self {
        Self(value.into_iter().map(PlainNodeId::from).collect())
    }
}

impl From<NodeSet> for Vec<PlainNodeId> {
    fn from(value: NodeSet) -> Self {
        value.0.into_iter().collect()
    }
}

impl From<NodeSet> for Box<[PlainNodeId]> {
    fn from(value: NodeSet) -> Self {
        value.0.into_iter().collect()
    }
}

impl<A: Into<PlainNodeId>> FromIterator<A> for NodeSet {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        Self(HashSet::from_iter(iter.into_iter().map(Into::into)))
    }
}

impl Display for NodeSet {
    /// The alternate format displays a *sorted* list of short-form plain node ids, suitable for human-friendly output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match f.alternate() {
            false => write_nodes(self, f),
            true => write_nodes_sorted(self, f),
        }
    }
}

fn write_nodes(node_set: &NodeSet, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "[")?;
    let mut nodes = node_set.0.iter();
    if let Some(node) = nodes.next() {
        write!(f, "{node}")?;
        for node in nodes {
            write!(f, ", {node}")?;
        }
    }
    write!(f, "]")
}

fn write_nodes_sorted(node_set: &NodeSet, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "[")?;
    let mut nodes = node_set.0.iter().sorted();
    if let Some(node) = nodes.next() {
        write!(f, "{node}")?;
        for node in nodes {
            write!(f, ", {node}")?;
        }
    }
    write!(f, "]")
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
    pub fn new(nodeset: &NodeSet, nodes_config: &NodesConfiguration) -> Self {
        Self(
            nodeset
                .iter()
                .copied()
                .filter(|node_id| !nodes_config.get_log_server_storage_state(node_id).empty())
                .collect(),
        )
    }
}
