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
use tracing::info;

use restate_cluster_controller::ClusterControllerHandle;
use restate_metadata_store::MetadataStoreClient;
use restate_node_services::cluster_ctrl::cluster_ctrl_svc_server::ClusterCtrlSvc;
use restate_node_services::cluster_ctrl::{ClusterStateRequest, ClusterStateResponse};

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
            .map_err(|_| tonic::Status::aborted("Node is shutting down"))?;

        // todo: remove this and return the actual state via protobuf
        info!("Cluster state: {:?}", cluster_state);

        Ok(Response::new(ClusterStateResponse::default()))
    }
}
