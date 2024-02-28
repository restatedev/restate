// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::FullInvocationId;
use crate::invocation::ResponseResult;
use crate::GenerationalNodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IngressResponse {
    pub target_node: GenerationalNodeId,
    pub full_invocation_id: FullInvocationId,
    pub response: ResponseResult,
}
