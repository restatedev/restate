// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tonic::{async_trait, Request, Response, Status};
use tracing::debug;

use restate_node_services::cluster_ctrl::cluster_ctrl_svc_server::ClusterCtrlSvc;
use restate_node_services::cluster_ctrl::{AttachmentRequest, AttachmentResponse};

pub struct ClusterCtrlSvcHandler {}

impl ClusterCtrlSvcHandler {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ClusterCtrlSvc for ClusterCtrlSvcHandler {
    async fn attach_node(
        &self,
        request: Request<AttachmentRequest>,
    ) -> Result<Response<AttachmentResponse>, Status> {
        let node_id = request.into_inner().node_id.expect("node id must be set");
        debug!("Attaching node '{:?}'", node_id);
        Ok(Response::new(AttachmentResponse {}))
    }
}
