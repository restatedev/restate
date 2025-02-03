// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::Json;
use http::StatusCode;
use okapi_operation::openapi;

use crate::rest_api::error::GenericRestError;
use restate_core::network::net_util::create_tonic_channel;
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_client::NodeCtlSvcClient;
use restate_core::{my_node_id, Metadata};
use restate_types::config::Configuration;
use restate_types::{NodeId, PlainNodeId};

/// Cluster state endpoint
#[openapi(
    summary = "Cluster health",
    description = "Get the cluster health.",
    operation_id = "cluster_health",
    tags = "cluster_health"
)]
pub async fn cluster_health() -> Result<Json<ClusterHealthResponse>, GenericRestError> {
    let nodes_configuration = Metadata::with_current(|m| m.nodes_config_ref());
    let node_config = nodes_configuration
        .find_node_by_id(my_node_id())
        .map_err(|_| {
            GenericRestError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "The cluster does not seem to be provisioned yet. Try again later.",
            )
        })?;

    let mut node_ctl_svc_client = NodeCtlSvcClient::new(create_tonic_channel(
        node_config.address.clone(),
        &Configuration::pinned().networking,
    ));
    let cluster_health = node_ctl_svc_client
        .cluster_health(())
        .await
        .map_err(|err| GenericRestError::new(StatusCode::INTERNAL_SERVER_ERROR, err.message()))?
        .into_inner();

    let cluster_health_response = ClusterHealthResponse::from(cluster_health);

    Ok(Json(cluster_health_response))
}

#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema, prost_dto::FromProst,
)]
#[prost(target = "restate_core::protobuf::node_ctl_svc::ClusterHealthResponse")]
pub struct ClusterHealthResponse {
    /// Cluster name
    pub cluster_name: String,
    /// Embedded metadata cluster health if it was enabled
    pub metadata_cluster_health: Option<EmbeddedMetadataClusterHealth>,
}

#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema, prost_dto::FromProst,
)]
#[prost(target = "restate_core::protobuf::node_ctl_svc::EmbeddedMetadataClusterHealth")]
pub struct EmbeddedMetadataClusterHealth {
    /// Current members of the embedded metadata cluster
    #[from_prost(map = node_id_to_plain_node_id)]
    pub members: Vec<PlainNodeId>,
}

fn node_id_to_plain_node_id(node_id: restate_types::protobuf::common::NodeId) -> PlainNodeId {
    NodeId::from(node_id).id()
}
