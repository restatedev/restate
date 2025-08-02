// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::BTreeMap, time::Duration};

use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_encoding::{BilrostNewType, NetSerde};

use super::ServiceTag;
use crate::GenerationalNodeId;
use crate::net::{
    bilrost_wire_codec, bilrost_wire_codec_with_v1_fallback, define_rpc, define_service,
    define_unary_message,
};
use crate::partitions::state::{LeadershipState, ReplicaSetState};
use crate::time::MillisSinceEpoch;
use crate::{cluster::cluster_state::PartitionProcessorStatus, identifiers::PartitionId};

pub struct GossipService;

define_service! {
    @service = GossipService,
    @tag = ServiceTag::GossipService,
}

define_rpc! {
    @request = GetNodeState,
    @response = NodeStateResponse,
    @service = GossipService,
}

bilrost_wire_codec_with_v1_fallback!(GetNodeState);
bilrost_wire_codec_with_v1_fallback!(NodeStateResponse);

define_unary_message! {
    @message = Gossip,
    @service = GossipService,
}
bilrost_wire_codec!(Gossip);

define_rpc! {
    @request = GetClusterState,
    @response = ClusterStateReply,
    @service = GossipService,
}
bilrost_wire_codec!(GetClusterState);
bilrost_wire_codec!(ClusterStateReply);

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, bilrost::Message, NetSerde)]
pub struct GetNodeState {}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message, NetSerde)]
pub struct NodeStateResponse {
    /// Partition processor status per partition. Is set to None if this node is not a `Worker` node
    #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
    #[bilrost(1)]
    pub partition_processor_state: Option<BTreeMap<PartitionId, PartitionProcessorStatus>>,

    /// node uptime.
    // serde(default) is required for backward compatibility when updating the cluster,
    // ensuring that older nodes can still interact with newer nodes that recognize this attribute.
    #[serde(default)]
    #[bilrost(2)]
    pub uptime: Duration,
}

#[derive(Debug, Clone, BilrostNewType, NetSerde)]
pub struct GossipFlags(u32);
bitflags! {
    impl GossipFlags: u32 {
        /// This message is a special message, no information about peers will be included
        const Special = 1 << 0;
        /// Gossip message is for a bring-up. no `nodes` should be expected in this message.
        /// Needs to be combined with `Special`
        const BringUp = 1 << 1;
        /// flag for bring-up to indicate that the node has fully started. If unset, it means that
        /// the node is still starting up.
        /// Needs to be combined with `Special`
        const ReadyToServe = 1 << 2;
        /// The node is broadcasting that it's failing over (shutting down) and it's asking for
        /// peers to gracefully fail over to another node.
        ///
        /// This can be used in conjunction with `Special` or it can be set on regular gossip
        /// messages.
        const FailingOver = 1 << 3;
        /// Node didn't receive gossip messages for some _configurable_ time, yet it's able
        /// to send gossips, this could happen in asymmetric network failures. In this case, we
        /// don't consider this node (the sender) as alive and we don't count the gossip message
        /// as a valid liveness signal, but we still use peer information within the message as usual.
        ///
        /// This is *not* a `Special` message
        const FeelingLonely = 1 << 4;
        /// This gossip message contains the merged partition state, otherwise, extra
        /// fields like `partitions` should be ignored.
        const Enriched = 1 << 5;
    }
}

/// A gossip message sent between nodes to drive the failure detector
///
/// Note: The sender of the message is inferred from the network message envelope
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Gossip {
    /// The sender's startup time in milliseconds since epoch. This value must be unique and
    /// incrementing for each restart of the same node.
    #[bilrost(1)]
    pub instance_ts: MillisSinceEpoch,
    /// Timestamp at sender when this message was constructed. Used to measure delay/clock-skew between nodes
    /// and to fence against processing old messages.
    #[bilrost(2)]
    pub sent_at: MillisSinceEpoch,
    #[bilrost(3)]
    pub flags: GossipFlags,
    /// Information about the cluster form the sender's point of view. It also includes the sender itself, albeit
    /// not every field of the sender's state will be respected.
    #[bilrost(4)]
    pub nodes: Vec<Node>,
    /// Extra optional information about the nodes in the cluster
    /// Ignored if `Enriched` flag is not set on the message.
    #[bilrost(5)]
    pub partitions: Vec<PartitionReplicaSet>,
}

#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Node {
    #[bilrost(1)]
    pub instance_ts: MillisSinceEpoch,
    #[bilrost(2)]
    pub node_id: GenerationalNodeId,
    /// How many gossip intervals have passed since the last time we heard from/about this node (directly or
    /// indirectly)
    #[bilrost(3)]
    pub gossip_age: u32,
    #[bilrost(4)]
    pub in_failover: bool,
}

#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct PartitionReplicaSet {
    pub id: PartitionId,
    pub current_leader: LeadershipState,
    pub observed_current_membership: ReplicaSetState,
    pub observed_next_membership: Option<ReplicaSetState>,
}

// ** RPC for fetching cluster state from a peer node

#[derive(Debug, Clone, Copy, Default, bilrost::Message, NetSerde)]
pub struct GetClusterState;

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash, bilrost::Enumeration, NetSerde)]
#[repr(u8)]
pub enum CsReplyStatus {
    /// Response should not be used, the node's FailureDetector is not fully running yet or in
    /// lonely state.
    #[default]
    NotReady = 0,
    Ok = 1,
}

/// This is a full copy of the node's in-memory view of cluster state
#[derive(Debug, Default, Clone, bilrost::Message, NetSerde)]
pub struct ClusterStateReply {
    #[bilrost(1)]
    pub status: CsReplyStatus,
    #[bilrost(2)]
    pub nodes: Vec<CsNode>,
    #[bilrost(3)]
    pub partitions: Vec<PartitionReplicaSet>,
}

impl ClusterStateReply {
    pub fn not_ready() -> Self {
        Self {
            status: CsReplyStatus::NotReady,
            nodes: Vec::default(),
            partitions: Vec::default(),
        }
    }
}

#[derive(
    Debug, Default, Clone, Copy, Eq, PartialEq, Hash, bilrost::Enumeration, NetSerde, strum::Display,
)]
#[repr(u8)]
#[strum(serialize_all = "kebab-case")]
pub enum NodeState {
    #[default]
    Dead = 0,
    Alive = 1,
    FailingOver = 2,
}

#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct CsNode {
    #[bilrost(1)]
    pub node_id: GenerationalNodeId,
    #[bilrost(2)]
    pub state: NodeState,
}

impl NodeState {
    pub fn is_alive(self) -> bool {
        matches!(self, Self::Alive)
    }
}
