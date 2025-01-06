// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indexmap::IndexSet;
use restate_types::{GenerationalNodeId, NodeId, PlainNodeId};
use tokio::sync::watch;

use crate::{Metadata, TaskCenter};

//todo(azmy): Default is temporary, remove it when the implementation is done
#[derive(Debug, Clone, Default)]
pub struct ClusterState {
    watch: watch::Sender<()>,
}

impl ClusterState {
    // this is just a place holder since cluster state
    // should only be constructed via the failure detector
    pub fn new() -> Self {
        Self {
            watch: watch::Sender::new(()),
        }
    }

    pub fn try_current() -> Option<Self> {
        TaskCenter::with_current(|h| h.cluster_state())
    }

    #[track_caller]
    pub fn current() -> Self {
        TaskCenter::with_current(|h| h.cluster_state()).expect("called outside task-center scope")
    }

    pub fn watch(&self) -> watch::Receiver<()> {
        self.watch.subscribe()
    }

    /// Gets an iterator over all alive nodes
    pub fn alive(&self) -> impl Iterator<Item = GenerationalNodeId> {
        // Dummy implementation

        // assumes all nodes are alive
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        nodes_config
            .iter()
            .map(|(_, n)| n.current_generation)
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Gets an iterator over all dead nodes
    pub fn dead(&self) -> impl Iterator<Item = PlainNodeId> {
        // Dummy implementation

        // assumes all nodes are alive
        std::iter::empty()
    }

    /// Checks if a node is a live, returning its generation node id if it is.
    pub fn is_alive(&self, node_id: NodeId) -> Option<GenerationalNodeId> {
        // Dummy implementation

        // assume all nodes are alive
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        nodes_config
            .find_node_by_id(node_id)
            .map(|n| n.current_generation)
            .ok()
    }

    /// Finds the first alive node in the given slice.
    pub fn first_alive(&self, nodes: &[PlainNodeId]) -> Option<GenerationalNodeId> {
        // Dummy implementation

        // assumes all nodes are alive hence
        // always return the first node
        if nodes.is_empty() {
            return None;
        }

        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        nodes_config
            .find_node_by_id(nodes[0])
            .map(|n| n.current_generation)
            .ok()
    }

    /// Returns the subset of alive nodes from the given node set preserving their order.
    pub fn intersect(&self, nodes: &IndexSet<PlainNodeId>) -> IndexSet<GenerationalNodeId> {
        // Dummy implementation

        // this dummy implementation just assumes
        // all nodes in the set are alive and return the
        // current known generational id
        let nodes_config = Metadata::with_current(|m| m.nodes_config_ref());
        nodes
            .iter()
            .filter_map(|plain_id| {
                nodes_config
                    .find_node_by_id(*plain_id)
                    .map(|n| n.current_generation)
                    .ok()
            })
            .collect()
    }
}
