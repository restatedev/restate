// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Counts that summarize a [`ClusterView`].

use itertools::Itertools;
use serde::Serialize;

use super::model::{ClusterView, Liveness, PartitionHealth, SequencerState};

#[derive(Serialize)]
pub struct Summary {
    pub nodes: NodeSummary,
    pub partitions: PartitionSummary,
    pub logs: LogSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataSummary>,
}

#[derive(Serialize)]
pub struct NodeSummary {
    pub total: usize,
    pub alive: usize,
    pub dead: usize,
}

#[derive(Serialize)]
pub struct PartitionSummary {
    pub total: usize,
    /// with an active leader
    pub led: usize,
    pub healthy: usize,
    pub reconfiguring: usize,
    pub degraded: usize,
    pub unavailable: usize,
    /// without the cluster state, the processors and so the partitions' health are unknown
    pub unknown: usize,
}

#[derive(Serialize)]
pub struct LogSummary {
    pub total: usize,
    pub sealed: usize,
    /// logs whose sequencer is dead or restarted
    pub sequencer_down: usize,
    /// logs that can't take appends
    pub down: usize,
}

#[derive(Serialize)]
pub struct MetadataSummary {
    pub members: usize,
    /// members that report themselves as members
    pub up: usize,
    pub leader: Option<String>,
}

impl Summary {
    pub fn new(view: &ClusterView) -> Self {
        let health = |h: PartitionHealth| view.partitions.iter().filter(|p| p.health == h).count();

        let metadata = view
            .metadata_members()
            .filter(|m| !m.members.is_empty())
            .map(|m| MetadataSummary {
                members: m.members.len(),
                up: m.up,
                leader: view
                    .nodes
                    .iter()
                    .filter_map(|n| n.metadata_server.as_ref()?.leader)
                    .counts()
                    .into_iter()
                    .max_by_key(|(_, count)| *count)
                    .map(|(leader, _)| leader.to_string()),
            });

        Summary {
            nodes: NodeSummary {
                total: view.nodes.len(),
                alive: view.alive_nodes(),
                dead: view
                    .nodes
                    .iter()
                    .filter(|n| n.liveness == Liveness::Dead)
                    .count(),
            },
            partitions: PartitionSummary {
                total: view.partitions.len(),
                led: view.partitions.iter().filter(|p| p.is_led()).count(),
                healthy: health(PartitionHealth::Healthy),
                reconfiguring: health(PartitionHealth::Reconfiguring),
                degraded: health(PartitionHealth::Degraded),
                unavailable: health(PartitionHealth::Unavailable),
                unknown: health(PartitionHealth::Unknown),
            },
            logs: LogSummary {
                total: view.logs.len(),
                sealed: view.logs.iter().filter(|l| l.sealed).count(),
                sequencer_down: view
                    .logs
                    .iter()
                    .filter(|l| {
                        matches!(
                            l.sequencer_state,
                            Some(SequencerState::Dead | SequencerState::Restarted)
                        )
                    })
                    .count(),
                down: view.logs.iter().filter(|l| l.is_down()).count(),
            },
            metadata,
        }
    }
}
