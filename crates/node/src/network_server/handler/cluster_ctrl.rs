// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tonic_0_10::{async_trait, Request, Response, Status};
use tracing::info;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_server::ClusterCtrlSvc;
use restate_admin::cluster_controller::protobuf::{
    ClusterStateRequest, ClusterStateResponse, TrimLogRequest,
};
use restate_admin::cluster_controller::ClusterControllerHandle;
use restate_metadata_store::MetadataStoreClient;
use restate_types::logs::{LogId, Lsn};

use crate::network_server::AdminDependencies;

pub struct ClusterCtrlSvcHandler {
    _metadata_store_client: MetadataStoreClient,
    controller_handle: ClusterControllerHandle,
}

impl ClusterCtrlSvcHandler {
    pub fn new(admin_deps: AdminDependencies) -> Self {
        Self {
            controller_handle: admin_deps.cluster_controller_handle,
            _metadata_store_client: admin_deps.metadata_store_client,
        }
    }
}

#[async_trait]
impl ClusterCtrlSvc for ClusterCtrlSvcHandler {
    async fn get_cluster_state(
        &self,
        _request: Request<ClusterStateRequest>,
    ) -> Result<Response<ClusterStateResponse>, Status> {
        let cluster_state = self
            .controller_handle
            .get_cluster_state()
            .await
            .map_err(|_| tonic_0_10::Status::aborted("Node is shutting down"))?;

        let resp = ClusterStateResponse {
            cluster_state: Some((*cluster_state).clone().into()),
        };
        Ok(Response::new(resp))
    }

    /// Internal operations API to trigger the log truncation
    async fn trim_log(&self, request: Request<TrimLogRequest>) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let log_id = LogId::from(request.log_id);
        let trim_point = Lsn::from(request.trim_point);
        if let Err(err) = self
            .controller_handle
            .trim_log(log_id, trim_point)
            .await
            .map_err(|_| Status::aborted("Node is shutting down"))?
        {
            info!("Failed trimming the log: {err}");
            return Err(Status::internal(err.to_string()));
        }
        Ok(Response::new(()))
    }
}
