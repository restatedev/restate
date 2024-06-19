// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::cluster::cluster_state::RunMode;
use crate::identifiers::PartitionId;
use crate::net::{RequestId, TargetName};
use crate::partition_table::KeyRange;

use crate::net::define_rpc;

define_rpc! {
    @request = AttachRequest,
    @response = AttachResponse,
    @request_target = TargetName::AttachRequest,
    @response_target = TargetName::AttachResponse,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AttachRequest {
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachResponse {
    pub request_id: RequestId,
    pub actions: Vec<Action>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    RunPartition(RunPartition),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunPartition {
    pub partition_id: PartitionId,
    pub key_range_inclusive: KeyRange,
    pub mode: RunMode,
}
