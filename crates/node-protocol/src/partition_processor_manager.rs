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

use restate_types::identifiers::PartitionId;
use restate_types::processors::PartitionProcessorStatus;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::common::{RequestId, TargetName};
use crate::define_rpc;

define_rpc! {
    @request = GetProcessorsState,
    @response = ProcessorsStateResponse,
    @request_target = TargetName::GetProcessorsStateRequest,
    @response_target = TargetName::ProcessorsStateResponse,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GetProcessorsState {
    pub request_id: RequestId,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorsStateResponse {
    pub request_id: RequestId,
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    pub state: BTreeMap<PartitionId, PartitionProcessorStatus>,
}
