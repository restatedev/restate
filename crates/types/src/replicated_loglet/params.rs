// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use serde_with::DisplayFromStr;

use crate::nodes_config::NodesConfiguration;
use crate::{GenerationalNodeId, PlainNodeId};

use super::ReplicationProperty;

/// Configuration parameters of a replicated loglet segment
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct ReplicatedLogletParams {
    /// Unique identifier for this loglet
    pub loglet_id: ReplicatedLogletId,
    /// The sequencer node
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub sequencer: GenerationalNodeId,
    /// Replication properties of this loglet
    pub replication: ReplicationProperty,
    pub nodeset: NodeSet,
    /// The set of nodes the sequencer has been considering for writes after the last
    /// known_global_tail advance.
    ///
    /// If unset, the entire nodeset is considered as part of the write set
    /// If set, tail repair will attempt reading only from this set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_set: Option<NodeSet>,
}

impl ReplicatedLogletParams {
    pub fn deserialize_from(slice: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(slice)
    }

    pub fn serialize(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Clone,
    Copy,
    derive_more::From,
    derive_more::Deref,
    derive_more::Into,
    derive_more::Display,
)]
#[serde(transparent)]
#[repr(transparent)]
pub struct ReplicatedLogletId(u64);

impl ReplicatedLogletId {
    pub const fn new(id: u64) -> Self {
        Self(id)
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

impl Display for NodeSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, id) in self.0.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", id)?;
        }
        write!(f, "]")
    }
}

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

    /// returns true if this node didn't already exist in the nodeset
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

#[serde_with::serde_as]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    derive_more::Deref,
    derive_more::AsRef,
    derive_more::DerefMut,
    derive_more::IntoIterator,
    derive_more::Into,
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
