// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::service::ApplyMode;
use crate::service::Force;

use super::error::*;
use super::state::*;

use restate_meta_rest_model::deployments::*;
use restate_schema_api::deployment::DeploymentMetadataResolver;
use restate_service_client::Endpoint;
use restate_service_protocol::discovery::DiscoverEndpoint;
use restate_types::identifiers::InvalidLambdaARN;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{http, Json};
use okapi_operation::anyhow::Error;
use okapi_operation::okapi::openapi3::{MediaType, Responses};
use okapi_operation::okapi::Map;
use okapi_operation::*;
use serde::Deserialize;

/// Create deployment and return discovered services.
#[openapi(
    summary = "Create deployment",
    description = "Create deployment. Restate will invoke the endpoint to gather additional information required for registration, such as the services exposed by the deployment and their Protobuf descriptor. If the deployment is already registered, this method will fail unless `force` is set to `true`.",
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
pub async fn create_deployment<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    #[request_body(required = true)] Json(payload): Json<RegisterDeploymentRequest>,
) -> Result<impl IntoResponse, MetaApiError> {
    let address = match payload.deployment_metadata {
        RegisterDeploymentMetadata::Http { uri } => Endpoint::Http(uri, Default::default()),
        RegisterDeploymentMetadata::Lambda {
            arn,
            assume_role_arn,
        } => Endpoint::Lambda(
            arn.parse()
                .map_err(|e: InvalidLambdaARN| MetaApiError::InvalidField("arn", e.to_string()))?,
            assume_role_arn.map(Into::into),
        ),
    };
    let endpoint = DiscoverEndpoint::new(
        address,
        payload.additional_headers.unwrap_or_default().into(),
    );

    let apply_changes = if payload.dry_run {
        ApplyMode::DryRun
    } else {
        ApplyMode::Apply
    };

    let force = if payload.force { Force::Yes } else { Force::No };
    let registration_result = state
        .meta_handle()
        .register_deployment(endpoint, force, apply_changes)
        .await?;

    let response_body = RegisterDeploymentResponse {
        id: registration_result.deployment,
        services: registration_result.services,
    };

    Ok((
        StatusCode::CREATED,
        [(
            http::header::LOCATION,
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
pub async fn get_deployment<S: DeploymentMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(deployment_id): Path<String>,
) -> Result<Json<DetailedDeploymentResponse>, MetaApiError> {
    let (endpoint_meta, services) = state
        .schemas()
        .get_deployment_and_services(&deployment_id)
        .ok_or_else(|| MetaApiError::DeploymentNotFound(deployment_id.clone()))?;

    Ok(DetailedDeploymentResponse {
        id: deployment_id,
        deployment: endpoint_meta.into(),
        services,
    }
    .into())
}

/// Return deployment descriptors
#[openapi(
    summary = "Get deployment descriptors",
    description = "Get deployment Protobuf descriptor pool, serialized as protobuf type google.protobuf.FileDescriptorSet",
    operation_id = "get_deployment_descriptors",
    tags = "deployment",
    parameters(path(
        name = "deployment",
        description = "Deployment identifier",
        schema = "std::string::String"
    ))
)]
pub async fn get_deployment_descriptors<S: DeploymentMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(deployment_id): Path<String>,
) -> Result<ProtoBytes, MetaApiError> {
    Ok(ProtoBytes(
        state
            .schemas()
            .get_deployment_descriptor_pool(&deployment_id)
            .ok_or_else(|| MetaApiError::DeploymentNotFound(deployment_id.clone()))?,
    ))
}

/// List services
#[openapi(
    summary = "List deployments",
    description = "List all registered deployments.",
    operation_id = "list_deployments",
    tags = "deployment"
)]
pub async fn list_deployments<S: DeploymentMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
) -> Json<ListDeploymentResponse> {
    ListDeploymentResponse {
        deployments: state
            .schemas()
            .get_deployments()
            .into_iter()
            .map(|(endpoint_meta, services)| DeploymentResponse {
                id: endpoint_meta.id(),
                deployment: endpoint_meta.into(),
                services: services
                    .into_iter()
                    .map(|(name, revision)| ServiceNameRevPair { name, revision })
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
pub async fn delete_deployment<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(deployment_id): Path<String>,
    Query(DeleteDeploymentParams { force }): Query<DeleteDeploymentParams>,
) -> Result<StatusCode, MetaApiError> {
    if let Some(true) = force {
        state.meta_handle().remove_deployment(deployment_id).await?;
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
