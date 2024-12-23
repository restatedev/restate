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
use std::time::Instant;

use prost_dto::IntoProto;

use crate::cluster::cluster_state::PartitionProcessorStatus;
use crate::identifiers::PartitionId;
use crate::time::MillisSinceEpoch;
use crate::{GenerationalNodeId, PlainNodeId, Version};

/// A container for health information about every node and partition in the
/// cluster.
#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::deprecated_cluster::ClusterState")]
pub struct ClusterState {
    #[into_proto(map = "instant_to_proto")]
    pub last_refreshed: Option<Instant>,
    #[proto(required)]
    pub nodes_config_version: Version,
    #[proto(required)]
    pub partition_table_version: Version,
    #[proto(required)]
    pub logs_metadata_version: Version,
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
            NodeState::Dead(_) | NodeState::Suspect(_) => None,
        })
    }

    pub fn dead_nodes(&self) -> impl Iterator<Item = &PlainNodeId> {
        self.nodes.iter().flat_map(|(node_id, state)| match state {
            NodeState::Alive(_) | NodeState::Suspect(_) => None,
            NodeState::Dead(_) => Some(node_id),
        })
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn empty() -> Self {
        ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::INVALID,
            partition_table_version: Version::INVALID,
            logs_metadata_version: Version::INVALID,
            nodes: BTreeMap::default(),
        }
    }
}

fn instant_to_proto(t: Instant) -> prost_types::Duration {
    t.elapsed().try_into().unwrap()
}

#[derive(Debug, Clone, IntoProto)]
#[proto(
    target = "crate::protobuf::deprecated_cluster::NodeState",
    oneof = "state"
)]
pub enum NodeState {
    Alive(AliveNode),
    Dead(DeadNode),
    Suspect(SuspectNode),
}

#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::deprecated_cluster::AliveNode")]
pub struct AliveNode {
    #[proto(required)]
    pub last_heartbeat_at: MillisSinceEpoch,
    #[proto(required)]
    pub generational_node_id: GenerationalNodeId,
    pub partitions: BTreeMap<PartitionId, PartitionProcessorStatus>,
}

#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::deprecated_cluster::DeadNode")]
pub struct DeadNode {
    pub last_seen_alive: Option<MillisSinceEpoch>,
}

#[derive(Debug, Clone, IntoProto)]
#[proto(target = "crate::protobuf::deprecated_cluster::SuspectNode")]
/// As the name implies, SuspectNode is both dead and alive
/// until we receive a heartbeat
pub struct SuspectNode {
    #[proto(required)]
    pub generational_node_id: GenerationalNodeId,
    #[proto(required)]
    pub last_attempt: MillisSinceEpoch,
}
