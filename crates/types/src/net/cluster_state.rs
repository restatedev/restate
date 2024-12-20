// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{define_rpc, TargetName};
use serde::{Deserialize, Serialize};

define_rpc! {
    @request= NodePing,
    @response= NodePong,
    @request_target=TargetName::NodePing,
    @response_target=TargetName::NodePong,
}

/// Gossip Push message. Is pushed from each node to every other known node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePing {
    //todo(azmy): share latest metadata versions?
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePong {}
