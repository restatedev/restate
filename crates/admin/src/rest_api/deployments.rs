// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use std::time::SystemTime;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::{Extension, Json};
use http::{Method, Uri};
use restate_admin_rest_model::deployments::*;
use restate_admin_rest_model::version::AdminApiVersion;
use restate_errors::warn_it;
use restate_types::deployment::{HttpDeploymentAddress, LambdaDeploymentAddress};
use restate_types::identifiers::{DeploymentId, InvalidLambdaARN, ServiceRevision};
use restate_types::schema;
use restate_types::schema::deployment::{Deployment, DeploymentType};
use restate_types::schema::registry::{
    AddDeploymentResult, AllowBreakingChanges, ApplyMode, DiscoveryClient, MetadataService,
    Overwrite, TelemetryClient,
};
use restate_types::schema::service::ServiceMetadata;
use serde::Deserialize;

/// Register deployment
///
/// Registers a new deployment (HTTP or Lambda). Restate will invoke the endpoint to discover available services and handlers,
/// and make them available for invocation. For more information, see the [deployment documentation](https://docs.restate.dev/services/versioning#registering-a-deployment).
#[utoipa::path(
    post,
    path = "/deployments",
    operation_id = "create_deployment",
    tag = "deployment",
    request_body = RegisterDeploymentRequest,
    responses(
        (status = 200, description = "Deployment already exists. No change if force = false, services overwritten if force = true", body = RegisterDeploymentResponse, headers(
            ("Location" = String, description = "URI of the deployment")
        )),
        (status = 201, description = "Deployment created successfully and services discovered", body = RegisterDeploymentResponse, headers(
            ("Location" = String, description = "URI of the created deployment")
        )),
        MetaApiError
    )
)]
pub async fn create_deployment<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Extension(version): Extension<AdminApiVersion>,
    Json(payload): Json<RegisterDeploymentRequest>,
) -> Result<impl IntoResponse, MetaApiError>
where
    Metadata: MetadataService,
    Discovery: DiscoveryClient,
    Telemetry: TelemetryClient,
{
    // -- Bunch of data structures mapping back and forth
    let (force, breaking, dry_run) = match &payload {
        RegisterDeploymentRequest::Http {
            breaking,
            dry_run,
            force,
            ..
        } => (*force, *breaking, *dry_run),
        RegisterDeploymentRequest::Lambda {
            breaking,
            dry_run,
            force,
            ..
        } => (*force, *breaking, *dry_run),
    };
    let (allow_breaking, overwrite) =
        // Force defaults to true only in admin api version 1 or 2
        if force.unwrap_or(version == AdminApiVersion::V1 || version == AdminApiVersion::V2) {
            (AllowBreakingChanges::Yes, Overwrite::Yes)
        } else if breaking {
            (AllowBreakingChanges::Yes, Overwrite::No)
        } else {
            (AllowBreakingChanges::No, Overwrite::No)
        };
    let apply_mode = if dry_run {
        ApplyMode::DryRun
    } else {
        ApplyMode::Apply
    };
    let request = match payload {
        RegisterDeploymentRequest::Http {
            uri,
            additional_headers,
            metadata,
            use_http_11,
            ..
        } => {
            validate_uri(&uri)?;

            schema::registry::RegisterDeploymentRequest {
                deployment_address: HttpDeploymentAddress::new(uri).into(),
                additional_headers: additional_headers.unwrap_or_default().into(),
                metadata,
                use_http_11,
                allow_breaking,
                overwrite,
                apply_mode,
            }
        }
        RegisterDeploymentRequest::Lambda {
            arn,
            assume_role_arn,
            additional_headers,
            metadata,
            ..
        } => schema::registry::RegisterDeploymentRequest {
            deployment_address: LambdaDeploymentAddress::new(
                arn.parse().map_err(|e: InvalidLambdaARN| {
                    MetaApiError::InvalidField("arn", e.to_string())
                })?,
                assume_role_arn,
            )
            .into(),
            additional_headers: additional_headers.unwrap_or_default().into(),
            metadata,
            use_http_11: false,
            allow_breaking,
            overwrite,
            apply_mode,
        },
    };

    // -- Perform the registration with the schema registry
    let (result, deployment, services) = state
        .schema_registry
        .register_deployment(request)
        .await
        .inspect_err(|e| warn_it!(e))?;

    // -- Map response
    let status_code = match result {
        AddDeploymentResult::Created => StatusCode::CREATED,
        AddDeploymentResult::Unchanged => {
            if version == AdminApiVersion::Unknown || version.as_repr() >= 3 {
                StatusCode::OK
            } else {
                return Err(MetaApiError::Conflict(format!(
                    "deployment {} already exists",
                    deployment.id
                )));
            }
        }
        AddDeploymentResult::Overwritten => {
            if version == AdminApiVersion::Unknown || version.as_repr() >= 3 {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            }
        }
    };

    Ok((
        status_code,
        [(header::LOCATION, format!("deployments/{}", deployment.id))],
        Json(to_register_response(deployment, services)),
    ))
}

/// Get deployment
///
/// Returns detailed information about a registered deployment, including deployment metadata and the services it exposes.
#[utoipa::path(
    get,
    path = "/deployments/{deployment}",
    operation_id = "get_deployment",
    tag = "deployment",
    params(
        ("deployment" = String, Path, description = "Deployment identifier"),
    ),
    responses(
        (status = 200, description = "Deployment details including services and configuration", body = DetailedDeploymentResponse),
        MetaApiError
    )
)]
pub async fn get_deployment<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(deployment_id): Path<DeploymentId>,
) -> Result<Json<DetailedDeploymentResponse>, MetaApiError>
where
    Metadata: MetadataService,
{
    let (deployment, services) = state
        .schema_registry
        .get_deployment(deployment_id)
        .ok_or_else(|| MetaApiError::DeploymentNotFound(deployment_id))?;

    Ok(to_detailed_deployment_response(deployment, services).into())
}

/// List deployments
///
/// Returns a list of all registered deployments, including their endpoints and associated services.
#[utoipa::path(
    get,
    path = "/deployments",
    operation_id = "list_deployments",
    tag = "deployment",
    responses(
        (status = 200, description = "List of all registered deployments with their metadata", body = ListDeploymentsResponse)
    )
)]
pub async fn list_deployments<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
) -> Json<ListDeploymentsResponse>
where
    Metadata: MetadataService,
{
    let deployments = state
        .schema_registry
        .list_deployments()
        .into_iter()
        .map(|(deployment, services)| to_deployment_response(deployment, services))
        .collect();

    ListDeploymentsResponse { deployments }.into()
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct DeleteDeploymentParams {
    /// If true, the deployment will be forcefully deleted. This might break in-flight invocations, use with caution.
    pub force: Option<bool>,
}

/// Delete deployment
///
/// Delete a deployment. Currently, only forced deletions are supported.
/// **Use with caution**: forcing a deployment deletion can break in-flight invocations.
#[utoipa::path(
    delete,
    path = "/deployments/{deployment}",
    operation_id = "delete_deployment",
    tag = "deployment",
    params(
        ("deployment" = String, Path, description = "Deployment identifier"),
        DeleteDeploymentParams
    ),
    responses(
        (status = 202, description = "Deployment deletion accepted and will be processed asynchronously"),
        (status = 501, description = "Not implemented. Graceful deployment deletion (force=false) is not yet supported.", body = ErrorDescriptionResponse),
        MetaApiError
    )
)]
pub async fn delete_deployment<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(deployment_id): Path<DeploymentId>,
    Query(DeleteDeploymentParams { force }): Query<DeleteDeploymentParams>,
) -> Result<StatusCode, MetaApiError>
where
    Metadata: MetadataService,
{
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

/// Update deployment
///
/// Updates an existing deployment configuration, such as the endpoint address or invocation headers.
/// By default, service schemas are not re-discovered. Set `overwrite: true` to trigger re-discovery.
#[utoipa::path(
    method(put, patch),
    path = "/deployments/{deployment}",
    operation_id = "update_deployment",
    tag = "deployment",
    params(
        ("deployment" = String, Path, description = "Deployment identifier"),
    ),
    responses(
        (status = 200, description = "Deployment updated successfully. Address and invocation options are updated. Service schemas are only updated if overwrite was set to true.", body = DetailedDeploymentResponse),
        MetaApiError
    )
)]
pub async fn update_deployment<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Extension(version): Extension<AdminApiVersion>,
    method: Method,
    Path(deployment_id): Path<DeploymentId>,
    Json(payload): Json<UpdateDeploymentRequest>,
) -> Result<Json<DetailedDeploymentResponse>, MetaApiError>
where
    Metadata: MetadataService,
    Discovery: DiscoveryClient,
{
    if (version >= AdminApiVersion::V3 || version == AdminApiVersion::Unknown)
        && method == Method::PUT
    {
        return Err(MetaApiError::DeprecatedPutDeployment);
    }

    // -- Bunch of data structures mapping back and forth
    let (overwrite, dry_run) = match &payload {
        UpdateDeploymentRequest::Http {
            overwrite, dry_run, ..
        } => (*overwrite, *dry_run),
        UpdateDeploymentRequest::Lambda {
            overwrite, dry_run, ..
        } => (*overwrite, *dry_run),
    };
    let overwrite = if overwrite {
        Overwrite::Yes
    } else {
        Overwrite::No
    };
    let apply_mode = if dry_run {
        ApplyMode::DryRun
    } else {
        ApplyMode::Apply
    };
    let (update_deployment_address, additional_headers) = match payload {
        UpdateDeploymentRequest::Http {
            uri,
            additional_headers,
            use_http_11,
            ..
        } => {
            if uri.is_none() && additional_headers.is_none() && use_http_11.is_none() {
                // No changes to do, just return 200
                let (deployment, services) = state
                    .schema_registry
                    .get_deployment(deployment_id)
                    .ok_or_else(|| MetaApiError::DeploymentNotFound(deployment_id))?;

                return Ok(to_detailed_deployment_response(deployment, services).into());
            }

            if let Some(uri) = &uri {
                validate_uri(uri)?;
            }

            (
                if uri.is_none() && use_http_11.is_none() {
                    None
                } else {
                    Some(schema::registry::UpdateDeploymentAddress::Http { uri, use_http_11 })
                },
                additional_headers,
            )
        }
        UpdateDeploymentRequest::Lambda {
            arn,
            assume_role_arn,
            additional_headers,
            ..
        } => {
            if arn.is_none() && additional_headers.is_none() && assume_role_arn.is_none() {
                // No changes to do, just return 200
                let (deployment, services) = state
                    .schema_registry
                    .get_deployment(deployment_id)
                    .ok_or_else(|| MetaApiError::DeploymentNotFound(deployment_id))?;

                return Ok(to_detailed_deployment_response(deployment, services).into());
            }

            (
                if arn.is_none() && assume_role_arn.is_none() {
                    None
                } else {
                    Some(schema::registry::UpdateDeploymentAddress::Lambda {
                        arn: arn
                            .map(|a| {
                                a.parse().map_err(|e: InvalidLambdaARN| {
                                    MetaApiError::InvalidField("arn", e.to_string())
                                })
                            })
                            .transpose()?,
                        assume_role_arn,
                    })
                },
                additional_headers,
            )
        }
    };

    let (deployment, services) = state
        .schema_registry
        .update_deployment(
            deployment_id,
            schema::registry::UpdateDeploymentRequest {
                update_deployment_address,
                additional_headers: additional_headers.map(Into::into),
                overwrite,
                apply_mode,
            },
        )
        .await
        .inspect_err(|e| warn_it!(e))?;

    Ok(Json(to_detailed_deployment_response(deployment, services)))
}

fn to_register_response(
    Deployment {
        id,
        supported_protocol_versions,
        sdk_version,
        info,
        ..
    }: Deployment,
    services: Vec<ServiceMetadata>,
) -> RegisterDeploymentResponse {
    RegisterDeploymentResponse {
        id,
        services,
        min_protocol_version: *supported_protocol_versions.start(),
        max_protocol_version: *supported_protocol_versions.end(),
        sdk_version,
        info,
    }
}

fn to_deployment_response(
    Deployment {
        id,
        ty,
        additional_headers,
        supported_protocol_versions,
        sdk_version,
        created_at,
        metadata,
        info,
        ..
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
            additional_headers: additional_headers.into(),
            metadata,
            created_at: SystemTime::from(created_at).into(),
            min_protocol_version: *supported_protocol_versions.start(),
            max_protocol_version: *supported_protocol_versions.end(),
            sdk_version,
            services: services
                .into_iter()
                .map(|(name, revision)| ServiceNameRevPair { name, revision })
                .collect(),
            info,
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
            additional_headers: additional_headers.into(),
            metadata,
            created_at: SystemTime::from(created_at).into(),
            min_protocol_version: *supported_protocol_versions.start(),
            max_protocol_version: *supported_protocol_versions.end(),
            sdk_version,
            services: services
                .into_iter()
                .map(|(name, revision)| ServiceNameRevPair { name, revision })
                .collect(),
            info,
        },
    }
}

fn to_detailed_deployment_response(
    Deployment {
        id,
        ty,
        additional_headers,
        supported_protocol_versions,
        sdk_version,
        created_at,
        metadata,
        info,
        ..
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
            additional_headers: additional_headers.into(),
            metadata,
            created_at: SystemTime::from(created_at).into(),
            min_protocol_version: *supported_protocol_versions.start(),
            max_protocol_version: *supported_protocol_versions.end(),
            sdk_version,
            services,
            info,
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
            additional_headers: additional_headers.into(),
            metadata,
            created_at: SystemTime::from(created_at).into(),
            min_protocol_version: *supported_protocol_versions.start(),
            max_protocol_version: *supported_protocol_versions.end(),
            sdk_version,
            services,
            info,
        },
    }
}

#[inline]
#[allow(clippy::result_large_err)]
fn validate_uri(uri: &Uri) -> Result<(), MetaApiError> {
    if uri.scheme().is_none() || uri.authority().is_none() {
        return Err(MetaApiError::InvalidField(
            "uri",
            format!("The provided uri {uri} is not absolute, only absolute URIs can be used."),
        ));
    }
    Ok(())
}
