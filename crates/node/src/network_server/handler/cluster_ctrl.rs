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

use restate_node_services::cluster_ctrl::cluster_ctrl_svc_server::ClusterCtrlSvc;
use restate_node_services::cluster_ctrl::{PartitionsStatusRequest, PartitionsStatusResponse};

pub struct ClusterCtrlSvcHandler {}

impl ClusterCtrlSvcHandler {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ClusterCtrlSvc for ClusterCtrlSvcHandler {
    async fn get_partitions_status(
        &self,
        _request: Request<PartitionsStatusRequest>,
    ) -> Result<Response<PartitionsStatusResponse>, Status> {
        unimplemented!()
    }
}
