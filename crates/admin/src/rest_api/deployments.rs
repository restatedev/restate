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
use std::time::SystemTime;

use crate::schema_registry;
use crate::schema_registry::{ApplyMode, RegisterDeploymentResult};
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::{Extension, Json};
use http::Uri;
use okapi_operation::*;
use restate_admin_rest_model::deployments::*;
use restate_admin_rest_model::version::AdminApiVersion;
use restate_errors::warn_it;
use restate_types::deployment::{HttpDeploymentAddress, LambdaDeploymentAddress};
use restate_types::identifiers::{DeploymentId, InvalidLambdaARN, ServiceRevision};
use restate_types::schema::deployment::{Deployment, DeploymentMetadata, DeploymentType};
use restate_types::schema::service::ServiceMetadata;
use restate_types::schema::updater;
use serde::Deserialize;

/// Create deployment and return discovered services.
#[openapi(
    summary = "Create deployment",
    description = "Create and register a new deployment. \
    Restate will invoke the endpoint to gather additional information required for registration, such as the services exposed by the deployment. \
    If the deployment is already registered, this method will return 200 and no changes will be made. \
    If the deployment updates some already existing services, schema breaking changes checks will run. If you want to bypass them, use `breaking: true`. \
    To overwrite an already existing deployment, use `force: true`",
    operation_id = "create_deployment",
    tags = "deployment",
    external_docs(url = "https://docs.restate.dev/operate/registration"),
    responses(
        ignore_return_type = true,
        response(
            status = "200",
            description = "Already exists. No change if force = false, overwritten if force = true",
            content = "Json<RegisterDeploymentResponse>",
        ),
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
    Extension(version): Extension<AdminApiVersion>,
    #[request_body(required = true)] Json(payload): Json<RegisterDeploymentRequest>,
) -> Result<impl IntoResponse, MetaApiError> {
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
            (updater::AllowBreakingChanges::Yes, updater::Overwrite::Yes)
        } else if breaking {
            (updater::AllowBreakingChanges::Yes, updater::Overwrite::No)
        } else {
            (updater::AllowBreakingChanges::No, updater::Overwrite::No)
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

            schema_registry::RegisterDeploymentRequest {
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
        } => schema_registry::RegisterDeploymentRequest {
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
        RegisterDeploymentResult::Created => StatusCode::CREATED,
        RegisterDeploymentResult::Unchanged => {
            if version == AdminApiVersion::Unknown || version.as_repr() >= 3 {
                StatusCode::OK
            } else {
                return Err(MetaApiError::Conflict(format!(
                    "deployment {} already exists",
                    deployment.id
                )));
            }
        }
        RegisterDeploymentResult::Overwritten => {
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
    description = "Update an already existing deployment. \
    This lets you update the address and options when invoking the deployment, such as the additional headers for HTTP or the assume role for Lambda. \
    The registered services and handlers won't be overwritten, unless `overwrite: true`.",
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
        updater::Overwrite::Yes
    } else {
        updater::Overwrite::No
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
                if uri.is_none() || use_http_11.is_none() {
                    None
                } else {
                    Some(schema_registry::UpdateDeploymentAddress::Http { uri, use_http_11 })
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
                    Some(schema_registry::UpdateDeploymentAddress::Lambda {
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
            schema_registry::UpdateDeploymentRequest {
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
        metadata:
            DeploymentMetadata {
                supported_protocol_versions,
                sdk_version,
                ..
            },
    }: Deployment,
    services: Vec<ServiceMetadata>,
) -> RegisterDeploymentResponse {
    RegisterDeploymentResponse {
        id,
        services,
        min_protocol_version: *supported_protocol_versions.start(),
        max_protocol_version: *supported_protocol_versions.end(),
        sdk_version,
    }
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
                metadata,
                ..
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
            metadata,
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
            metadata,
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
                metadata,
                ..
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
            metadata,
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
            metadata,
            created_at: SystemTime::from(created_at).into(),
            min_protocol_version: *supported_protocol_versions.start(),
            max_protocol_version: *supported_protocol_versions.end(),
            sdk_version,
            services,
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
