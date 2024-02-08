// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::proto::cluster_controller_server::ClusterController;
use crate::proto::{AttachmentRequest, AttachmentResponse};
use restate_types::NodeId;
use tonic::{async_trait, Request, Response, Status};
use tracing::debug;

pub struct Handler {}

impl Handler {
    pub fn new() -> Self {
        Handler {}
    }
}

#[async_trait]
impl ClusterController for Handler {
    async fn attach_node(
        &self,
        request: Request<AttachmentRequest>,
    ) -> Result<Response<AttachmentResponse>, Status> {
        let node_id = request.into_inner().node_id.expect("node_id must be set");
        debug!("Register node '{}'", NodeId::from(node_id));
        Ok(Response::new(AttachmentResponse {}))
    }
}
