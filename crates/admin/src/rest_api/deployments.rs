// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use std::collections::HashMap;
use std::time::SystemTime;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use http::uri::Scheme;
use okapi_operation::*;
use restate_admin_rest_model::deployments::*;
use restate_errors::warn_it;
use restate_service_client::Endpoint;
use restate_service_protocol::discovery::DiscoverEndpoint;
use restate_types::identifiers::{DeploymentId, InvalidLambdaARN, ServiceRevision};
use restate_types::schema::deployment::{Deployment, DeploymentMetadata, DeploymentType};
use restate_types::schema::service::ServiceMetadata;
use restate_types::schema::updater;
use serde::Deserialize;

/// Create deployment and return discovered services.
#[openapi(
    summary = " pubCreate deployment",
    description = "Create deployment. Restate will invoke the endpoint to gather additional information required for registration, such as the services exposed by the deployment. If the deployment is already registered, this method will fail unless `force` is set to `true`.",
    operation_id = "create_deployment",
    tags = "deployment",
    external_docs(url = "https://docs.restate.dev/operate/registration"),
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
pub async fn create_deployment<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    #[request_body(required = true)] Json(payload): Json<RegisterDeploymentRequest>,
) -> Result<impl IntoResponse, MetaApiError> {
    let (discover_endpoint, routing_header, force, dry_run) = match payload {
        RegisterDeploymentRequest::Http {
            uri,
            additional_headers,
            routing_header,
            use_http_11,
            force,
            dry_run,
        } => {
            // Verify URI is absolute!
            if uri.scheme().is_none() || uri.authority().is_none() {
                return Err(MetaApiError::InvalidField(
                    "uri",
                    format!(
                        "The provided uri {uri} is not absolute, only absolute URIs can be used."
                    ),
                ));
            }

            let is_using_https = uri.scheme().unwrap() == &Scheme::HTTPS;

            // Add it as part of the additional headers
            let mut additional_headers: HashMap<_, _> =
                additional_headers.unwrap_or_default().into();
            if let Some(routing_header) = &routing_header {
                additional_headers.insert(routing_header.key.clone(), routing_header.value.clone());
            }

            (
                DiscoverEndpoint::new(
                    Endpoint::Http(
                        uri,
                        if use_http_11 {
                            Some(http::Version::HTTP_11)
                        } else if is_using_https {
                            // ALPN will sort this out
                            None
                        } else {
                            // By default, we use h2c on HTTP
                            Some(http::Version::HTTP_2)
                        },
                    ),
                    additional_headers,
                ),
                routing_header.map(|h| (h.key, h.value)),
                force,
                dry_run,
            )
        }
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
                    None,
                ),
                additional_headers.unwrap_or_default().into(),
            ),
            None,
            force,
            dry_run,
        ),
    };

    let force = if force {
        updater::Force::Yes
    } else {
        updater::Force::No
    };

    let apply_mode = if dry_run {
        updater::ApplyMode::DryRun
    } else {
        updater::ApplyMode::Apply
    };

    let (deployment, services) = state
        .schema_registry
        .register_deployment(discover_endpoint, routing_header, force, apply_mode)
        .await
        .inspect_err(|e| warn_it!(e))?;

    let response_body = RegisterDeploymentResponse {
        id: deployment.id,
        services,
        min_protocol_version: *deployment.metadata.supported_protocol_versions.start(),
        max_protocol_version: *deployment.metadata.supported_protocol_versions.end(),
        sdk_version: deployment.metadata.sdk_version,
    };

    Ok((
        StatusCode::CREATED,
        [(
            header::LOCATION,
            format!("deployments/{}", response_body.id),
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
pub async fn get_deployment<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(deployment_id): Path<DeploymentId>,
) -> Result<Json<DetailedDeploymentResponse>, MetaApiError> {
    let (deployment, services) = state
        .schema_registry
        .get_deployment(deployment_id)
        .ok_or_else(|| MetaApiError::DeploymentNotFound(deployment_id))?;

    Ok(to_detailed_deployment_response(deployment, services).into())
}

/// List deployments
#[openapi(
    summary = "List deployments",
    description = "List all registered deployments.",
    operation_id = "list_deployments",
    tags = "deployment"
)]
pub async fn list_deployments<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
) -> Json<ListDeploymentsResponse> {
    let deployments = state
        .schema_registry
        .list_deployments()
        .into_iter()
        .map(|(deployment, services)| to_deployment_response(deployment, services))
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
pub async fn delete_deployment<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(deployment_id): Path<DeploymentId>,
    Query(DeleteDeploymentParams { force }): Query<DeleteDeploymentParams>,
) -> Result<StatusCode, MetaApiError> {
    if let Some(true) = force {
        state
            .schema_registry
            .delete_deployment(deployment_id)
            .await
            .inspect_err(|e| warn_it!(e))?;
        Ok(StatusCode::ACCEPTED)
    } else {
        Ok(StatusCode::NOT_IMPLEMENTED)
    }
}

/// Update a deployment
#[openapi(
    summary = "Update deployment",
    description = "Update deployment. Invokes the endpoint and replaces the existing deployment metadata with the discovered information. This is a dangerous operation that should be used only when there are failing invocations on the deployment that cannot be resolved any other way. Sense checks are applied to test that the new deployment is sufficiently similar to the old one.",
    operation_id = "update_deployment",
    tags = "deployment",
    external_docs(url = "https://docs.restate.dev/operate/versioning"),
    parameters(path(
        name = "deployment",
        description = "Deployment identifier",
        schema = "std::string::String"
    ))
)]
pub async fn update_deployment<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(deployment_id): Path<DeploymentId>,
    #[request_body(required = true)] Json(payload): Json<UpdateDeploymentRequest>,
) -> Result<Json<DetailedDeploymentResponse>, MetaApiError> {
    let (discover_endpoint, dry_run) = match payload {
        UpdateDeploymentRequest::Http {
            uri,
            additional_headers,
            use_http_11,
            dry_run,
        } => {
            // Verify URI is absolute!
            if uri.scheme().is_none() || uri.authority().is_none() {
                return Err(MetaApiError::InvalidField(
                    "uri",
                    format!(
                        "The provided uri {uri} is not absolute, only absolute URIs can be used."
                    ),
                ));
            }

            let is_using_https = uri.scheme().unwrap() == &Scheme::HTTPS;

            (
                DiscoverEndpoint::new(
                    Endpoint::Http(
                        uri,
                        if use_http_11 {
                            Some(http::Version::HTTP_11)
                        } else if is_using_https {
                            // ALPN will sort this out
                            None
                        } else {
                            // By default, we use h2c on HTTP
                            Some(http::Version::HTTP_2)
                        },
                    ),
                    additional_headers.unwrap_or_default().into(),
                ),
                dry_run,
            )
        }
        UpdateDeploymentRequest::Lambda {
            arn,
            assume_role_arn,
            additional_headers,
            dry_run,
        } => (
            DiscoverEndpoint::new(
                Endpoint::Lambda(
                    arn.parse().map_err(|e: InvalidLambdaARN| {
                        MetaApiError::InvalidField("arn", e.to_string())
                    })?,
                    assume_role_arn.map(Into::into),
                    None,
                ),
                additional_headers.unwrap_or_default().into(),
            ),
            dry_run,
        ),
    };

    let apply_mode = if dry_run {
        updater::ApplyMode::DryRun
    } else {
        updater::ApplyMode::Apply
    };

    let (deployment, services) = state
        .schema_registry
        .update_deployment(deployment_id, discover_endpoint, apply_mode)
        .await
        .inspect_err(|e| warn_it!(e))?;

    Ok(Json(to_detailed_deployment_response(deployment, services)))
}

fn to_deployment_response(
    Deployment {
        id,
        metadata:
            DeploymentMetadata {
                ty,
                delivery_options,
                supported_protocol_versions,
                sdk_version,
                created_at,
            },
    }: Deployment,
    services: Vec<(String, ServiceRevision)>,
) -> DeploymentResponse {
    match ty {
        DeploymentType::Http {
            http_version,
            protocol_type,
            address,
        } => DeploymentResponse::Http {
            id,
            uri: address,
            protocol_type,
            http_version,
            additional_headers: delivery_options.additional_headers.into(),
            created_at: SystemTime::from(created_at).into(),
            min_protocol_version: *supported_protocol_versions.start(),
            max_protocol_version: *supported_protocol_versions.end(),
            sdk_version,
            services: services
                .into_iter()
                .map(|(name, revision)| ServiceNameRevPair { name, revision })
                .collect(),
        },
        DeploymentType::Lambda {
            arn,
            assume_role_arn,
            compression,
        } => DeploymentResponse::Lambda {
            id,
            arn,
            assume_role_arn: assume_role_arn.map(Into::into),
            compression,
            additional_headers: delivery_options.additional_headers.into(),
            created_at: SystemTime::from(created_at).into(),
            min_protocol_version: *supported_protocol_versions.start(),
            max_protocol_version: *supported_protocol_versions.end(),
            sdk_version,
            services: services
                .into_iter()
                .map(|(name, revision)| ServiceNameRevPair { name, revision })
                .collect(),
        },
    }
}

fn to_detailed_deployment_response(
    Deployment {
        id,
        metadata:
            DeploymentMetadata {
                ty,
                delivery_options,
                supported_protocol_versions,
                sdk_version,
                created_at,
            },
    }: Deployment,
    services: Vec<ServiceMetadata>,
) -> DetailedDeploymentResponse {
    match ty {
        DeploymentType::Http {
            http_version,
            protocol_type,
            address,
        } => DetailedDeploymentResponse::Http {
            id,
            uri: address,
            protocol_type,
            http_version,
            additional_headers: delivery_options.additional_headers.into(),
            created_at: SystemTime::from(created_at).into(),
            min_protocol_version: *supported_protocol_versions.start(),
            max_protocol_version: *supported_protocol_versions.end(),
            sdk_version,
            services,
        },
        DeploymentType::Lambda {
            arn,
            assume_role_arn,
            compression,
        } => DetailedDeploymentResponse::Lambda {
            id,
            arn,
            assume_role_arn: assume_role_arn.map(Into::into),
            compression,
            additional_headers: delivery_options.additional_headers.into(),
            created_at: SystemTime::from(created_at).into(),
            min_protocol_version: *supported_protocol_versions.start(),
            max_protocol_version: *supported_protocol_versions.end(),
            sdk_version,
            services,
        },
    }
}
