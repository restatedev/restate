// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::error::*;
use crate::state::AdminServiceState;

use crate::rest_api::log_error;
use crate::schema_registry::{ApplyMode, Force};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use okapi_operation::*;
use restate_admin_rest_model::deployments::*;
use restate_service_client::Endpoint;
use restate_service_protocol::discovery::DiscoverEndpoint;
use restate_types::identifiers::{DeploymentId, InvalidLambdaARN};
use serde::Deserialize;

/// Create deployment and return discovered services.
#[openapi(
    summary = "Create deployment",
    description = "Create deployment. Restate will invoke the endpoint to gather additional information required for registration, such as the services exposed by the deployment. If the deployment is already registered, this method will fail unless `force` is set to `true`.",
    operation_id = "create_deployment",
    tags = "deployment",
    responses(
        ignore_return_type = true,
        response(
            status = "201",
            description = "Created",
            content = "Json<RegisterDeploymentResponse>",
        ),
        from_type = "MetaApiError",
    )
)]
pub async fn create_deployment<V>(
    State(state): State<AdminServiceState<V>>,
    #[request_body(required = true)] Json(payload): Json<RegisterDeploymentRequest>,
) -> Result<impl IntoResponse, MetaApiError> {
    let (discover_endpoint, force, dry_run) = match payload {
        RegisterDeploymentRequest::Http {
            uri,
            additional_headers,
            use_http_11,
            force,
            dry_run,
        } => (
            DiscoverEndpoint::new(
                Endpoint::Http(
                    uri,
                    if use_http_11 {
                        http::Version::HTTP_11
                    } else {
                        http::Version::HTTP_2
                    },
                ),
                additional_headers.unwrap_or_default().into(),
            ),
            force,
            dry_run,
        ),
        RegisterDeploymentRequest::Lambda {
            arn,
            assume_role_arn,
            additional_headers,
            force,
            dry_run,
        } => (
            DiscoverEndpoint::new(
                Endpoint::Lambda(
                    arn.parse().map_err(|e: InvalidLambdaARN| {
                        MetaApiError::InvalidField("arn", e.to_string())
                    })?,
                    assume_role_arn.map(Into::into),
                ),
                additional_headers.unwrap_or_default().into(),
            ),
            force,
            dry_run,
        ),
    };

    let force = if force { Force::Yes } else { Force::No };

    let apply_mode = if dry_run {
        ApplyMode::DryRun
    } else {
        ApplyMode::Apply
    };

    let (id, services) = log_error(
        state
            .schema_registry
            .register_deployment(discover_endpoint, force, apply_mode)
            .await,
    )?;

    let response_body = RegisterDeploymentResponse { id, services };

    Ok((
        StatusCode::CREATED,
        [(
            header::LOCATION,
            format!("/deployments/{}", response_body.id),
        )],
        Json(response_body),
    ))
}

/// Return deployment
#[openapi(
    summary = "Get deployment",
    description = "Get deployment metadata",
    operation_id = "get_deployment",
    tags = "deployment",
    parameters(path(
        name = "deployment",
        description = "Deployment identifier",
        schema = "std::string::String"
    ))
)]
pub async fn get_deployment<V>(
    State(state): State<AdminServiceState<V>>,
    Path(deployment_id): Path<DeploymentId>,
) -> Result<Json<DetailedDeploymentResponse>, MetaApiError> {
    let (deployment, services) = state
        .schema_registry
        .get_deployment(deployment_id)
        .ok_or_else(|| MetaApiError::DeploymentNotFound(deployment_id))?;

    Ok(DetailedDeploymentResponse {
        id: deployment.id,
        deployment: deployment.metadata.into(),
        services,
    }
    .into())
}

/// List deployments
#[openapi(
    summary = "List deployments",
    description = "List all registered deployments.",
    operation_id = "list_deployments",
    tags = "deployment"
)]
pub async fn list_deployments<V>(
    State(state): State<AdminServiceState<V>>,
) -> Json<ListDeploymentsResponse> {
    let deployments = state
        .schema_registry
        .list_deployments()
        .into_iter()
        .map(|(deployment, services)| DeploymentResponse {
            id: deployment.id,
            deployment: deployment.metadata.into(),
            services: services
                .into_iter()
                .map(|(name, revision)| ServiceNameRevPair { name, revision })
                .collect(),
        })
        .collect();

    ListDeploymentsResponse { deployments }.into()
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteDeploymentParams {
    pub force: Option<bool>,
}

/// Discover endpoint and return discovered endpoints.
#[openapi(
    summary = "Delete deployment",
    description = "Delete deployment. Currently it's supported to remove a deployment only using the force flag",
    operation_id = "delete_deployment",
    tags = "deployment",
    parameters(
        path(
            name = "deployment",
            description = "Deployment identifier",
            schema = "std::string::String"
        ),
        query(
            name = "force",
            description = "If true, the deployment will be forcefully deleted. This might break in-flight invocations, use with caution.",
            required = false,
            style = "simple",
            allow_empty_value = false,
            schema = "bool",
        )
    ),
    responses(
        ignore_return_type = true,
        response(
            status = "202",
            description = "Accepted",
            content = "okapi_operation::Empty",
        ),
        response(
            status = "501",
            description = "Not implemented. Only using the force flag is supported at the moment.",
            content = "okapi_operation::Empty",
        ),
        from_type = "MetaApiError",
    )
)]
pub async fn delete_deployment<V>(
    State(state): State<AdminServiceState<V>>,
    Path(deployment_id): Path<DeploymentId>,
    Query(DeleteDeploymentParams { force }): Query<DeleteDeploymentParams>,
) -> Result<StatusCode, MetaApiError> {
    if let Some(true) = force {
        log_error(state.schema_registry.delete_deployment(deployment_id).await)?;
        Ok(StatusCode::ACCEPTED)
    } else {
        Ok(StatusCode::NOT_IMPLEMENTED)
    }
}
