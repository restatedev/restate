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

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::TargetName;
use crate::{cluster::cluster_state::PartitionProcessorStatus, identifiers::PartitionId};

super::define_rpc! {
    @request=GetNodeState,
    @response=NodeStateResponse,
    @request_target=TargetName::NodeGetNodeStateRequest,
    @response_target=TargetName::NodeGetNodeStateResponse,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct GetNodeState {}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStateResponse {
    /// State of paritions processor per parition. Is set to None if this node is not a `Worker` node
    #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
    pub paritions_processor_state: Option<BTreeMap<PartitionId, PartitionProcessorStatus>>,
}
