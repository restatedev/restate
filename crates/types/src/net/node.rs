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

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::TargetName;
use crate::{identifiers::PartitionId, partition_processor::PartitionProcessorStatus};

super::define_rpc! {
    @request=GetPartitionsProcessorsState,
    @response=PartitionsProcessorsStateResponse,
    @request_target=TargetName::NodeGetPartitionsProcessorsStateRequest,
    @response_target=TargetName::NodeGetPartitionsProcessorsStateResponse,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct GetPartitionsProcessorsState {}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionsProcessorsStateResponse {
    /// State of paritions processor per parition. Is set to None if this node is not a `Worker` node
    #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
    pub partition_processor_state: Option<BTreeMap<PartitionId, PartitionProcessorStatus>>,
}
