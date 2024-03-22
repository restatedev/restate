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
use super::notify_node_about_schema_changes;
use crate::state::AdminServiceState;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use okapi_operation::anyhow::Error;
use okapi_operation::okapi::openapi3::{MediaType, Responses};
use okapi_operation::okapi::Map;
use okapi_operation::*;
use restate_meta::{ApplyMode, Force};
use restate_meta_rest_model::deployments::*;
use restate_schema_api::deployment::DeploymentResolver;
use restate_service_client::Endpoint;
use restate_service_protocol::discovery::DiscoverEndpoint;
use restate_types::identifiers::InvalidLambdaARN;
use serde::Deserialize;

/// Create deployment and return discovered services.
#[openapi(
    summary = "Create deployment",
    description = "Create deployment. Restate will invoke the endpoint to gather additional information required for registration, such as the components exposed by the deployment. If the deployment is already registered, this method will fail unless `force` is set to `true`.",
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
pub async fn create_deployment(
    State(state): State<AdminServiceState>,
    #[request_body(required = true)] Json(payload): Json<RegisterDeploymentRequest>,
) -> Result<impl IntoResponse, MetaApiError> {
    let (discover_endpoint, force, dry_run) = match payload {
        RegisterDeploymentRequest::Http {
            uri,
            additional_headers,
            force,
            dry_run,
        } => (
            DiscoverEndpoint::new(
                Endpoint::Http(uri, Default::default()),
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

    let apply_changes = if dry_run {
        ApplyMode::DryRun
    } else {
        ApplyMode::Apply
    };

    let force = if force { Force::Yes } else { Force::No };
    let registration_result = state
        .meta_handle()
        .register_deployment(discover_endpoint, force, apply_changes)
        .await?;

    notify_node_about_schema_changes(state.schema_reader(), state.node_svc_client()).await;

    let response_body = RegisterDeploymentResponse {
        id: registration_result.deployment,
        components: registration_result.components,
    };

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
pub async fn get_deployment(
    State(state): State<AdminServiceState>,
    Path(deployment_id): Path<DeploymentId>,
) -> Result<Json<DetailedDeploymentResponse>, MetaApiError> {
    let (deployment, components) = state
        .schemas()
        .get_deployment_and_components(&deployment_id)
        .ok_or_else(|| MetaApiError::DeploymentNotFound(deployment_id))?;

    Ok(DetailedDeploymentResponse {
        id: deployment.id,
        deployment: deployment.metadata.into(),
        components,
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
pub async fn list_deployments(
    State(state): State<AdminServiceState>,
) -> Json<ListDeploymentsResponse> {
    ListDeploymentsResponse {
        deployments: state
            .schemas()
            .get_deployments()
            .into_iter()
            .map(|(deployment, services)| DeploymentResponse {
                id: deployment.id,
                deployment: deployment.metadata.into(),
                components: services
                    .into_iter()
                    .map(|(name, revision)| ComponentNameRevPair { name, revision })
                    .collect(),
            })
            .collect(),
    }
    .into()
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
pub async fn delete_deployment(
    State(state): State<AdminServiceState>,
    Path(deployment_id): Path<DeploymentId>,
    Query(DeleteDeploymentParams { force }): Query<DeleteDeploymentParams>,
) -> Result<StatusCode, MetaApiError> {
    if let Some(true) = force {
        state.meta_handle().remove_deployment(deployment_id).await?;
        notify_node_about_schema_changes(state.schema_reader(), state.node_svc_client()).await;
        Ok(StatusCode::ACCEPTED)
    } else {
        Ok(StatusCode::NOT_IMPLEMENTED)
    }
}

pub struct ProtoBytes(Bytes);

impl IntoResponse for ProtoBytes {
    fn into_response(self) -> Response {
        (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            )],
            self.0,
        )
            .into_response()
    }
}

impl ToMediaTypes for ProtoBytes {
    fn generate(_components: &mut Components) -> Result<Map<String, MediaType>, anyhow::Error> {
        Ok(okapi::map! {
            "application/x-protobuf".into() => {
                MediaType { ..Default::default() }
            }
        })
    }
}

impl ToResponses for ProtoBytes {
    fn generate(_components: &mut Components) -> Result<Responses, Error> {
        Ok(Responses::default())
    }
}
