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
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use super::ReplicationProperty;
use crate::logs::metadata::SegmentIndex;
use crate::logs::LogId;
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
)]
#[serde(transparent)]
#[repr(transparent)]
pub struct ReplicatedLogletId(u64);

impl ReplicatedLogletId {
    /// Creates a new [`ReplicatedLogletId`] from a [`LogId`] and a [`SegmentIndex`]. The upper
    /// 32 bits are the log_id and the lower are the segment_index.
    pub fn new(log_id: LogId, segment_index: SegmentIndex) -> Self {
        let id = u64::from(u32::from(log_id)) << 32 | u64::from(u32::from(segment_index));
        Self(id)
    }

    /// It's your responsibility that the value has the right meaning.
    pub const fn new_unchecked(v: u64) -> Self {
        Self(v)
    }

    /// Creates a new [`ReplicatedLogletId`] by incrementing the lower 32 bits (segment index part).
    pub fn next(&self) -> Self {
        assert!(
            self.0 & 0xFFFFFFFF < u64::from(u32::MAX),
            "Segment part must not overflow into the LogId part"
        );
        Self(self.0 + 1)
    }

    fn log_id(&self) -> LogId {
        LogId::new(u32::try_from(self.0 >> 32).expect("upper 32 bits should fit into u32"))
    }

    fn segment_index(&self) -> SegmentIndex {
        SegmentIndex::from(
            u32::try_from(self.0 & 0xFFFFFFFF).expect("lower 32 bits should fit into u32"),
        )
    }
}

impl Display for ReplicatedLogletId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.log_id(), self.segment_index())
    }
}

impl FromStr for ReplicatedLogletId {
    type Err = <u64 as FromStr>::Err;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains('_') {
            let parts: Vec<&str> = s.split('_').collect();
            let log_id: u32 = parts[0].parse()?;
            let segment_index: u32 = parts[1].parse()?;
            Ok(ReplicatedLogletId::new(
                LogId::from(log_id),
                SegmentIndex::from(segment_index),
            ))
        } else {
            // treat the string as raw replicated log-id
            let id: u64 = s.parse()?;
            Ok(ReplicatedLogletId(id))
        }
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

    /// Creates a new nodeset that excludes the nodes not in the provided set
    pub fn new_excluding(&self, excluded_node_ids: &HashSet<PlainNodeId, Xxh3Builder>) -> NodeSet {
        NodeSet::from(
            self.iter()
                .copied()
                .filter(|node_id| !excluded_node_ids.contains(node_id))
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
        write!(f, "{}", node)?;
        for node in nodes {
            write!(f, ", {}", node)?;
        }
    }
    write!(f, "]")
}

fn write_nodes_sorted(node_set: &NodeSet, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "[")?;
    let mut nodes = node_set.0.iter().sorted();
    if let Some(node) = nodes.next() {
        write!(f, "{}", node)?;
        for node in nodes {
            write!(f, ", {}", node)?;
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
