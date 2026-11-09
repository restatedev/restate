// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! How partitions and logs spread over the nodes, next to an even spread over the nodes that can
//! take them, to spot imbalances.

use serde::Serialize;

use restate_types::PlainNodeId;

use super::model::{ClusterView, Liveness, LogView, NodeView, PartitionView};

/// Partition leaders and replicas per node.
#[derive(Serialize)]
pub struct PartitionPlacement {
    pub nodes: Vec<PartitionShare>,
    /// leaders per node if they were spread evenly over the eligible nodes
    pub even_leaders: f64,
    /// replicas per node if they were spread evenly over the eligible nodes
    pub even_replicas: f64,
}

#[derive(Serialize)]
pub struct PartitionShare {
    #[serde(serialize_with = "display")]
    pub node: PlainNodeId,
    /// an alive worker in the active state, which can be given partitions
    pub eligible: bool,
    pub leaders: u32,
    /// replica set memberships, or running processors where replica sets are unknown
    pub replicas: u32,
}

/// Log sequencers and nodeset memberships per node.
#[derive(Serialize)]
pub struct LogPlacement {
    pub nodes: Vec<LogShare>,
    /// sequencers per node if they were spread evenly over the eligible nodes
    pub even_sequencers: f64,
    /// nodeset memberships per node if they were spread evenly over the eligible nodes
    pub even_nodesets: f64,
}

#[derive(Serialize)]
pub struct LogShare {
    #[serde(serialize_with = "display")]
    pub node: PlainNodeId,
    /// sequencers follow partition leaders: an alive, active worker
    pub sequencer_eligible: bool,
    /// an alive log-server that takes writes
    pub nodeset_eligible: bool,
    pub sequencers: u32,
    pub nodesets: u32,
}

fn alive_worker(node: &NodeView) -> bool {
    node.liveness == Liveness::Alive && node.worker_state.as_deref() == Some("active")
}

fn alive_log_server(node: &NodeView) -> bool {
    node.liveness == Liveness::Alive && node.storage_state.as_deref() == Some("read-write")
}

fn even(total: u32, eligible: usize) -> f64 {
    f64::from(total) / eligible.max(1) as f64
}

impl PartitionPlacement {
    /// Over the worker nodes, and nodes that host any of `partitions`.
    pub fn of(view: &ClusterView, partitions: &[&PartitionView]) -> Self {
        let nodes: Vec<PartitionShare> = view
            .nodes
            .iter()
            .map(|node| {
                let id = node.plain();
                let leaders = partitions
                    .iter()
                    .filter(|p| p.leader.is_some_and(|l| l.as_plain() == id))
                    .count() as u32;
                let replicas = partitions
                    .iter()
                    .filter(|p| match &p.membership {
                        Some(_) => p.is_member(id),
                        None => p.replica_on(id).is_some(),
                    })
                    .count() as u32;
                PartitionShare {
                    node: id,
                    eligible: alive_worker(node),
                    leaders,
                    replicas,
                }
            })
            .filter(|s| {
                s.leaders + s.replicas > 0
                    || view.node(s.node).is_some_and(|n| n.has_role("worker"))
            })
            .collect();
        let eligible = nodes.iter().filter(|s| s.eligible).count();
        Self {
            even_leaders: even(nodes.iter().map(|s| s.leaders).sum(), eligible),
            even_replicas: even(nodes.iter().map(|s| s.replicas).sum(), eligible),
            nodes,
        }
    }
}

impl LogPlacement {
    /// Over the nodes that can sequence or store logs, and nodes that do for any of `logs`.
    pub fn of(view: &ClusterView, logs: &[&LogView]) -> Self {
        let nodes: Vec<LogShare> = view
            .nodes
            .iter()
            .map(|node| {
                let id = node.plain();
                LogShare {
                    node: id,
                    sequencer_eligible: alive_worker(node),
                    nodeset_eligible: alive_log_server(node),
                    sequencers: logs
                        .iter()
                        .filter(|l| l.sequencer.is_some_and(|s| s.as_plain() == id))
                        .count() as u32,
                    nodesets: logs.iter().filter(|l| l.nodeset.contains(&id)).count() as u32,
                }
            })
            .filter(|s| {
                s.sequencers + s.nodesets > 0
                    || view
                        .node(s.node)
                        .is_some_and(|n| n.has_role("worker") || n.has_role("log-server"))
            })
            .collect();
        Self {
            even_sequencers: even(
                nodes.iter().map(|s| s.sequencers).sum(),
                nodes.iter().filter(|s| s.sequencer_eligible).count(),
            ),
            even_nodesets: even(
                nodes.iter().map(|s| s.nodesets).sum(),
                nodes.iter().filter(|s| s.nodeset_eligible).count(),
            ),
            nodes,
        }
    }
}

/// Whether `count` is off an even share by more than one, and by more than a fifth.
pub fn imbalanced(count: u32, even: f64) -> bool {
    let off = (f64::from(count) - even).abs();
    off > 1.0 && off > even * 0.2
}

fn display<S: serde::Serializer>(id: &PlainNodeId, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(id)
}
