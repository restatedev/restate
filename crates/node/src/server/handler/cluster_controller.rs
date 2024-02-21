// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_node_services::cluster_controller::cluster_controller_svc_server::ClusterControllerSvc;
use restate_node_services::cluster_controller::{AttachmentRequest, AttachmentResponse};
use tonic::{async_trait, Request, Response, Status};
use tracing::debug;

pub struct ClusterControllerHandler {}

impl ClusterControllerHandler {
    pub fn new() -> Self {
        ClusterControllerHandler {}
    }
}

#[async_trait]
impl ClusterControllerSvc for ClusterControllerHandler {
    async fn attach_node(
        &self,
        request: Request<AttachmentRequest>,
    ) -> Result<Response<AttachmentResponse>, Status> {
        let node_id = request.into_inner().node_id.expect("node id must be set");
        debug!("Attaching node '{:?}'", node_id);
        Ok(Response::new(AttachmentResponse {}))
    }
}
