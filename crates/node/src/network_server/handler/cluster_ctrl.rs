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

use restate_core::metadata::Metadata;
use restate_meta::MetaReader;
use restate_node_services::cluster_ctrl::cluster_ctrl_svc_server::ClusterCtrlSvc;
use restate_node_services::cluster_ctrl::{AttachmentRequest, AttachmentResponse};
use restate_node_services::cluster_ctrl::{FetchSchemasRequest, FetchSchemasResponse};

use crate::network_server::AdminDependencies;

pub struct ClusterCtrlSvcHandler {
    admin_deps: AdminDependencies,
    _metadata: Metadata,
}

impl ClusterCtrlSvcHandler {
    pub fn new(admin_deps: AdminDependencies, metadata: Metadata) -> Self {
        Self {
            admin_deps,
            _metadata: metadata,
        }
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

    async fn fetch_schemas(
        &self,
        _request: Request<FetchSchemasRequest>,
    ) -> Result<Response<FetchSchemasResponse>, Status> {
        let schema_updates = self.admin_deps.schema_reader.read().await.map_err(|err| {
            Status::internal(format!("Could not read schema information: '{}'", err))
        })?;

        let serialized_updates =
            bincode::serde::encode_to_vec(schema_updates, bincode::config::standard()).map_err(
                |err| {
                    Status::internal(format!("Could not serialize schema information: '{}'", err))
                },
            )?;

        Ok(Response::new(FetchSchemasResponse {
            schemas_bin: serialized_updates.into(),
        }))
    }
}
