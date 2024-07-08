// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Instant;

use prost_dto::IntoProto;
use serde::{Deserialize, Serialize};

use crate::identifiers::{LeaderEpoch, PartitionId};
use crate::logs::Lsn;
use crate::time::MillisSinceEpoch;
use crate::{GenerationalNodeId, PlainNodeId, Version};

/// A container for health information about every node and partition in the
/// cluster.
#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::cluster::ClusterState")]
pub struct ClusterState {
    #[into_proto(map = "instant_to_proto")]
    pub last_refreshed: Option<Instant>,
    #[proto(required)]
    pub nodes_config_version: Version,
    #[proto(required)]
    pub partition_table_version: Version,
    pub nodes: BTreeMap<PlainNodeId, NodeState>,
}

impl ClusterState {
    pub fn is_reliable(&self) -> bool {
        // todo: make this configurable
        // If the cluster state is older than 10 seconds, then it is not reliable.
        self.last_refreshed
            .map(|last_refreshed| last_refreshed.elapsed().as_secs() < 10)
            .unwrap_or(false)
    }

    pub fn alive_nodes(&self) -> impl Iterator<Item = &AliveNode> {
        self.nodes.values().flat_map(|node| match node {
            NodeState::Alive(alive_node) => Some(alive_node),
            NodeState::Dead(_) => None,
        })
    }

    pub fn dead_nodes(&self) -> impl Iterator<Item = &PlainNodeId> {
        self.nodes.iter().flat_map(|(node_id, state)| match state {
            NodeState::Alive(_) => None,
            NodeState::Dead(_) => Some(node_id),
        })
    }
}

fn instant_to_proto(t: Instant) -> prost_types::Duration {
    t.elapsed().try_into().unwrap()
}

#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::cluster::NodeState", oneof = "state")]
pub enum NodeState {
    Alive(AliveNode),
    Dead(DeadNode),
}

#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::cluster::AliveNode")]
pub struct AliveNode {
    #[proto(required)]
    pub last_heartbeat_at: MillisSinceEpoch,
    #[proto(required)]
    pub generational_node_id: GenerationalNodeId,
    pub partitions: BTreeMap<PartitionId, PartitionProcessorStatus>,
}

#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::cluster::DeadNode")]
pub struct DeadNode {
    pub last_seen_alive: Option<MillisSinceEpoch>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, IntoProto)]
#[proto(target = "crate::protobuf::cluster::RunMode")]
pub enum RunMode {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, IntoProto)]
#[proto(target = "crate::protobuf::cluster::ReplayStatus")]
pub enum ReplayStatus {
    Starting,
    Active,
    CatchingUp,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProto)]
#[proto(target = "crate::protobuf::cluster::PartitionProcessorStatus")]
pub struct PartitionProcessorStatus {
    #[proto(required)]
    pub updated_at: MillisSinceEpoch,
    pub planned_mode: RunMode,
    pub effective_mode: RunMode,
    pub last_observed_leader_epoch: Option<LeaderEpoch>,
    pub last_observed_leader_node: Option<GenerationalNodeId>,
    pub last_applied_log_lsn: Option<Lsn>,
    pub last_record_applied_at: Option<MillisSinceEpoch>,
    pub num_skipped_records: u64,
    pub replay_status: ReplayStatus,
    pub last_persisted_log_lsn: Option<Lsn>,
    // Set if replay_status is CatchingUp
    pub target_tail_lsn: Option<Lsn>,
}

impl Default for PartitionProcessorStatus {
    fn default() -> Self {
        Self {
            updated_at: MillisSinceEpoch::now(),
            planned_mode: RunMode::Follower,
            effective_mode: RunMode::Follower,
            last_observed_leader_epoch: None,
            last_observed_leader_node: None,
            last_applied_log_lsn: None,
            last_record_applied_at: None,
            num_skipped_records: 0,
            replay_status: ReplayStatus::Starting,
            last_persisted_log_lsn: None,
            target_tail_lsn: None,
        }
    }
}

impl PartitionProcessorStatus {
    pub fn is_effective_leader(&self) -> bool {
        self.effective_mode == RunMode::Leader
    }

    pub fn new() -> Self {
        Self::default()
    }
}
