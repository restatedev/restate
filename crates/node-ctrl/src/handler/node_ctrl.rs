// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_node_ctrl_proto::node_ctrl::node_ctrl_server::NodeCtrl;
use restate_node_ctrl_proto::node_ctrl::{IdentResponse, NodeStatus};
use tonic::{Request, Response, Status};

// -- GRPC Service Handlers --
pub struct NodeCtrlHandler {}

impl NodeCtrlHandler {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl NodeCtrl for NodeCtrlHandler {
    async fn get_ident(&self, _request: Request<()>) -> Result<Response<IdentResponse>, Status> {
        // STUB IMPLEMENTATION
        return Ok(Response::new(IdentResponse {
            status: NodeStatus::Alive.into(),
        }));
    }
}
