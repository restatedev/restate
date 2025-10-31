// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use prost_dto::IntoProst;
use serde::{Deserialize, Serialize};

use restate_encoding::NetSerde;
use restate_ty::partitions::{LeaderEpoch, PartitionId};

use crate::logs::Lsn;
use crate::time::MillisSinceEpoch;
use crate::{GenerationalNodeId, PlainNodeId, Version};

/// A container for health information about every node and partition in the
/// cluster.
#[derive(Debug, Clone, IntoProst)]
#[prost(target = "crate::protobuf::cluster::ClusterState")]
pub struct LegacyClusterState {
    #[into_prost(map = "instant_to_proto")]
    pub last_refreshed: Option<Instant>,
    #[prost(required)]
    pub nodes_config_version: Version,
    #[prost(required)]
    pub partition_table_version: Version,
    #[prost(required)]
    pub logs_metadata_version: Version,
    pub nodes: BTreeMap<PlainNodeId, NodeState>,
}

impl LegacyClusterState {
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

    #[cfg(feature = "test-util")]
    pub fn empty() -> Self {
        LegacyClusterState {
            last_refreshed: None,
            nodes_config_version: Version::INVALID,
            partition_table_version: Version::INVALID,
            logs_metadata_version: Version::INVALID,
            nodes: BTreeMap::default(),
        }
    }

    pub fn is_partition_processor_active(
        &self,
        partition_id: &PartitionId,
        node_id: &PlainNodeId,
    ) -> bool {
        self.nodes
            .get(node_id)
            .and_then(|node| match node {
                NodeState::Alive(alive) => alive
                    .partitions
                    .get(partition_id)
                    .map(|partition_state| partition_state.replay_status == ReplayStatus::Active),
                NodeState::Dead(_) => None,
            })
            .unwrap_or_default()
    }

    /// Returns true if the given node runs the partition processor leader for the given partition
    /// id. The decision is based on the partition processor reporting as their effective_mode
    /// `RunMode::Leader`.
    pub fn runs_partition_processor_leader(
        &self,
        node_id: &PlainNodeId,
        partition_id: &PartitionId,
    ) -> bool {
        self.nodes
            .get(node_id)
            .map(|node| match node {
                NodeState::Alive(alive) => alive
                    .partitions
                    .get(partition_id)
                    .map(|partition_state| partition_state.effective_mode == RunMode::Leader)
                    .unwrap_or_default(),
                NodeState::Dead(_) => false,
            })
            .unwrap_or_default()
    }
}

fn instant_to_proto(t: Instant) -> prost_types::Duration {
    t.elapsed().try_into().unwrap()
}

#[derive(Debug, Clone, IntoProst, strum::Display)]
#[prost(target = "crate::protobuf::cluster::NodeState", oneof = "state")]
#[strum(serialize_all = "snake_case")]
pub enum NodeState {
    Alive(AliveNode),
    // #[deprecated(since ="1.3.3", note = "Use restate_types::cluster_state::ClusterState instead for detecting dead nodes")]
    Dead(DeadNode),
}

#[derive(Debug, Clone, IntoProst)]
#[prost(target = "crate::protobuf::cluster::AliveNode")]
pub struct AliveNode {
    #[prost(required)]
    pub last_heartbeat_at: MillisSinceEpoch,
    #[prost(required)]
    pub generational_node_id: GenerationalNodeId,
    pub partitions: BTreeMap<PartitionId, PartitionProcessorStatus>,
    // age of daemon in seconds
    #[prost(name=uptime_s)]
    #[into_prost(map=Duration::as_secs, map_by_ref)]
    pub uptime: Duration,
}

#[derive(Debug, Clone, IntoProst)]
#[prost(target = "crate::protobuf::cluster::DeadNode")]
pub struct DeadNode {
    pub last_seen_alive: Option<MillisSinceEpoch>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    Eq,
    PartialEq,
    IntoProst,
    strum::Display,
    bilrost::Enumeration,
    NetSerde,
)]
#[prost(target = "crate::protobuf::cluster::RunMode")]
#[strum(serialize_all = "snake_case")]
pub enum RunMode {
    Follower = 0,
    Leader = 1,
}

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    IntoProst,
    strum::Display,
    bilrost::Enumeration,
    NetSerde,
)]
#[prost(target = "crate::protobuf::cluster::ReplayStatus")]
#[strum(serialize_all = "snake_case")]
pub enum ReplayStatus {
    Starting = 0,
    Active = 1,
    CatchingUp = 2,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoProst, bilrost::Message, NetSerde)]
#[prost(target = "crate::protobuf::cluster::PartitionProcessorStatus")]
pub struct PartitionProcessorStatus {
    #[prost(required)]
    #[bilrost(1)]
    pub updated_at: MillisSinceEpoch,
    #[bilrost(2)]
    pub planned_mode: RunMode,
    #[bilrost(3)]
    pub effective_mode: RunMode,
    #[bilrost(4)]
    pub last_observed_leader_epoch: Option<LeaderEpoch>,
    #[bilrost(5)]
    pub last_observed_leader_node: Option<GenerationalNodeId>,
    #[bilrost(6)]
    pub last_applied_log_lsn: Option<Lsn>,
    #[bilrost(7)]
    pub last_record_applied_at: Option<MillisSinceEpoch>,
    #[bilrost(8)]
    pub num_skipped_records: u64,
    #[bilrost(9)]
    pub replay_status: ReplayStatus,
    /// Also known as Durable LSN. Old name kept for compatibility with V1 networking clients.
    // todo: rename in 1.5 (or as soon as we no longer support interop with V1 cluster controllers)
    #[bilrost(10)]
    pub last_persisted_log_lsn: Option<Lsn>,
    #[bilrost(11)]
    pub last_archived_log_lsn: Option<Lsn>,
    // Set if replay_status is CatchingUp
    #[bilrost(12)]
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
            last_archived_log_lsn: None,
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
