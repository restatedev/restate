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

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::ServiceTag;
use crate::net::{default_wire_codec, define_rpc, define_service};
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

default_wire_codec!(GetNodeState);
default_wire_codec!(NodeStateResponse);

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct GetNodeState {}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStateResponse {
    /// Partition processor status per partition. Is set to None if this node is not a `Worker` node
    #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
    pub partition_processor_state: Option<BTreeMap<PartitionId, PartitionProcessorStatus>>,

    /// node uptime.
    // serde(default) is required for backward compatibility when updating the cluster,
    // ensuring that older nodes can still interact with newer nodes that recognize this attribute.
    #[serde(default)]
    pub uptime: Duration,
}
