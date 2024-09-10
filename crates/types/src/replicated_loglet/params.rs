// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo: remove when fleshed out
#![allow(unused)]

use std::collections::HashSet;
use std::num::NonZeroU8;

use serde_with::{DisplayFromStr, VecSkipError};

use crate::{GenerationalNodeId, PlainNodeId};

/// Configuration parameters of a replicated loglet segment
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct ReplicatedLogletParams {
    /// Unique identifier for this loglet
    loglet_id: ReplicatedLogletId,
    /// The sequencer node
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    sequencer: GenerationalNodeId,
    /// Replication properties of this loglet
    replication: Replication,
    nodeset: NodeSet,
    /// The set of nodes the sequencer has been considering for writes after the last
    /// known_global_tail advance.
    ///
    /// If unset, the entire nodeset is considered as part of the write set
    /// If set, tail repair will attempt reading only from this set.
    #[serde(skip_serializing_if = "Option::is_none")]
    write_set: Option<NodeSet>,
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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct Replication {
    /// The write-quorum for appends
    replication_factor: NonZeroU8,
    /// The number of extra copies (best effort) to be replicated in addition to the
    /// replication_factor.
    ///
    /// default is 0
    #[serde(default)]
    extra_copies: u8,
}

impl Replication {
    pub fn read_quorum_size(&self, nodeset: &NodeSet) -> u8 {
        // N - replication_factor + 1
        nodeset.len() - self.replication_factor.get() + 1
    }
}

#[serde_with::serde_as]
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct NodeSet(#[serde_as(as = "HashSet<DisplayFromStr>")] HashSet<PlainNodeId>);

impl NodeSet {
    pub fn from_single(node: PlainNodeId) -> Self {
        let mut set = HashSet::new();
        set.insert(node);
        Self(set)
    }

    pub fn len(&self) -> u8 {
        self.0
            .len()
            .try_into()
            .expect("nodeset cannot exceed 255 nodes")
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &PlainNodeId> {
        self.0.iter()
    }
}
