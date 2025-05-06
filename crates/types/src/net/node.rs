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

// Gossip protocol types

#[derive(Debug, Clone, BilrostNewType, NetSerde)]
pub struct GossipFlags(u32);
bitflags! {
    impl GossipFlags: u32 {
        /// This message is a special message, no information about peers will be included
        const Special = 1 << 0;
        /// Gossip message is for a bring-up. no `nodes` should be expected in this message.
        const BringUp = 1 << 1;
        /// flag for bring-up to indicate that the node has fully started. If unset, it means that
        /// the node is still starting up.
        const ReadyToServe = 1 << 2;
        /// The node is broadcasting that it's failing over (shutting down) and it's asking for
        /// peers to gracefully fail over to another node.
        ///
        /// This can be used in conjunction with Special or not
        const FailingOver = 1 << 3;
        /// Node didn't receive gossip messages for some _configurable_ time, yet it's able
        /// to send gossips, this could happen in asymmetric network failures. In this case, we
        /// don't consider this node (the sender) as alive and we don't count its message against
        /// it gossip age, but we still use the contents of the message as usual.
        /// The rest of the message is ignored if this flag is set.
        ///
        /// Also known as "lonely"
        const LongTimeSinceLastGossip = 1 << 4;
        /// This gossip message contains extra information about the nodes in the cluster.
        /// The field _extras_ can be respected.
        const Enriched = 1 << 5;
    }
}

#[derive(Debug, Clone, Default, Copy, Eq, PartialEq, bilrost::Enumeration, NetSerde)]
#[repr(u16)]
pub enum LifecycleState {
    #[default]
    Unknown = 0,
    Starting = 1,
    FailingOver = 2,
}

// 16 bytes
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
    // extras....
    // todo!()
    // also, maybe per-role health?
}

#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Extras {
    // todo!()
}

/// The sender of the message is the
#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Gossip {
    // the sender's startup time in milliseconds since epoch. This value must be unique and
    // incrementing for each restart of the same node.
    #[bilrost(1)]
    pub instance_ts: MillisSinceEpoch,
    // measure delay/timeskew between nodes
    #[bilrost(2)]
    pub sent_at: MillisSinceEpoch,
    // vector of nodes ordered by the nodes defined in the top of this message.
    #[bilrost(3)]
    pub flags: GossipFlags,
    // list of nodes in the cluster that we are talking about
    // everything in the rest of the message references this list by index
    #[bilrost(4)]
    pub nodes: Vec<Node>,

    // extras by node, mapped to the same nodes in `nodes`
    // Empty if `flags` does not contain `Enriched`
    #[bilrost(5)]
    pub extras: Vec<Extras>,
}
