// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::{Path, Query, State};
use axum::response::Response;
use restate_service_protocol_v4::serdes::SerdesRequest;
use restate_types::identifiers::DeploymentId;
use restate_types::schema::deployment::Deployment;
use restate_types::schema::registry::{MetadataService, SchemaRegistry};
use serde::Deserialize;
use tonic::service::AxumBody;

use super::error::MetaApiError;
use crate::state::AdminServiceState;

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct SerdesQueryParameters {
    /// If none, uses latest
    deployment: Option<DeploymentId>,
}

pub(crate) async fn decode<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path((service, serde_name)): Path<(String, String)>,
    Query(SerdesQueryParameters { deployment }): Query<SerdesQueryParameters>,
    body: AxumBody,
) -> Result<Response, MetaApiError>
where
    Metadata: MetadataService,
{
    let deployment = resolve_target_deployment(&state.schema_registry, &service, deployment)?;

    let response_body = state
        .serdes_client
        .decode(SerdesRequest {
            deployment,
            service_name: service,
            serde_name,
            body,
        })
        .await
        .map_err(|e| MetaApiError::Internal(e.to_string()))?;

    Ok(Response::builder()
        .body(AxumBody::new(response_body))
        .expect("builder should succeed"))
}

pub(crate) async fn encode<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path((service, serde_name)): Path<(String, String)>,
    Query(SerdesQueryParameters { deployment }): Query<SerdesQueryParameters>,
    body: AxumBody,
) -> Result<Response, MetaApiError>
where
    Metadata: MetadataService,
{
    let deployment = resolve_target_deployment(&state.schema_registry, &service, deployment)?;

    let response_body = state
        .serdes_client
        .encode(SerdesRequest {
            deployment,
            service_name: service,
            serde_name,
            body,
        })
        .await
        .map_err(|e| MetaApiError::Internal(e.to_string()))?;

    Ok(Response::builder()
        .body(AxumBody::new(response_body))
        .expect("builder should succeed"))
}

fn resolve_target_deployment<Metadata, Discovery, Telemetry>(
    schema_registry: &SchemaRegistry<Metadata, Discovery, Telemetry>,
    service_name: &str,
    deployment_id: Option<DeploymentId>,
) -> Result<Deployment, MetaApiError>
where
    Metadata: MetadataService,
{
    match deployment_id {
        Some(deployment_id) => schema_registry
            .get_deployment(deployment_id)
            .ok_or_else(|| MetaApiError::DeploymentNotFound(deployment_id)),
        None => schema_registry
            .resolve_latest_deployment_for_service(service_name)
            .ok_or_else(|| MetaApiError::ServiceNotFound(service_name.to_string())),
    }
}
