// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::SystemTime;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::{Extension, Json};
use http::{Method, Uri};
use serde::Deserialize;

use restate_admin_rest_model::deployments::*;
use restate_admin_rest_model::version::AdminApiVersion;
use restate_errors::warn_it;
use restate_types::deployment::{HttpAuth, HttpDeploymentAddress, LambdaDeploymentAddress};
use restate_types::identifiers::{DeploymentId, InvalidLambdaARN, ServiceRevision};
use restate_types::schema;
use restate_types::schema::deployment::{Deployment, DeploymentType};
use restate_types::schema::registry::{
    AddDeploymentResult, AllowBreakingChanges, ApplyMode, DiscoveryClient, MetadataService,
    Overwrite, TelemetryClient,
};
use restate_types::schema::service::ServiceMetadata;

use super::error::*;
use crate::rest_api::ErrorDescriptionResponse;
use crate::state::AdminServiceState;

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
            auth,
            ..
        } => {
            validate_uri(&uri)?;
            if let Some(auth) = &auth {
                validate_http_auth(&uri, auth, additional_headers.as_ref())?;
            }

            schema::registry::RegisterDeploymentRequest {
                deployment_address: HttpDeploymentAddress::new(uri).with_auth(auth).into(),
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
        .get_deployment_and_services(deployment_id)
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
    patch,
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
                    .get_deployment_and_services(deployment_id)
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
                    .get_deployment_and_services(deployment_id)
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
            auth,
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
            auth,
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
            auth,
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
            auth,
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

/// Validate the per-deployment `auth` field against REQ-VAL-01..02.
/// Per-field input hygiene (audience whitespace, service-account email
/// shape) was deliberately dropped: typos surface as Cloud Run 401s at
/// first invocation with a clearer error than any local regex could
/// give.
#[allow(clippy::result_large_err)]
fn validate_http_auth(
    uri: &Uri,
    _auth: &HttpAuth,
    additional_headers: Option<&restate_serde_util::SerdeableHeaderHashMap>,
) -> Result<(), MetaApiError> {
    // REQ-VAL-01: scheme must be https UNLESS host is loopback/private.
    let scheme_ok = uri
        .scheme()
        .map(|s| s.as_str().eq_ignore_ascii_case("https"))
        .unwrap_or(false);
    if !scheme_ok && !is_loopback_or_private_host(uri) {
        return Err(MetaApiError::InvalidField(
            "auth",
            format!(
                "GCP authentication requires an https URI for non-loopback/private hosts; got {uri}"
            ),
        ));
    }

    // REQ-VAL-02: when auth is set, additional_headers may carry either
    // Authorization OR X-Serverless-Authorization, but not both.
    if let Some(headers) = additional_headers {
        let map: std::collections::HashMap<http::HeaderName, http::HeaderValue> =
            headers.clone().into();
        let has_auth = map
            .keys()
            .any(|k| k.as_str().eq_ignore_ascii_case("authorization"));
        let has_xserv = map.keys().any(|k| {
            k.as_str()
                .eq_ignore_ascii_case("x-serverless-authorization")
        });
        if has_auth && has_xserv {
            return Err(MetaApiError::InvalidField(
                "additional_headers",
                "Cannot set both Authorization and X-Serverless-Authorization in \
                 additional_headers when GCP auth is enabled."
                    .to_owned(),
            ));
        }
    }

    // Loopback/private host with auth set is allowed by REQ-VAL-01's
    // exception clause; emit a warning so operators see it in node
    // logs. Not a numbered requirement, just useful for ops.
    if is_loopback_or_private_host(uri) {
        tracing::warn!(
            uri = %uri,
            "GCP auth configured for a loopback/private deployment URI; \
             tokens will still be minted and attached"
        );
    }

    Ok(())
}

fn is_loopback_or_private_host(uri: &Uri) -> bool {
    let Some(authority) = uri.authority() else {
        return false;
    };
    let host = authority.host();
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    // Strip IPv6 brackets if present.
    let host_clean = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    if let Ok(ipv4) = host_clean.parse::<std::net::Ipv4Addr>() {
        return ipv4.is_loopback() || ipv4.is_private();
    }
    if let Ok(ipv6) = host_clean.parse::<std::net::Ipv6Addr>() {
        // Loopback, ULA (fc00::/7), or link-local (fe80::/10).
        if ipv6.is_loopback() {
            return true;
        }
        let segs = ipv6.segments();
        let ula = (segs[0] & 0xfe00) == 0xfc00;
        let ll = (segs[0] & 0xffc0) == 0xfe80;
        return ula || ll;
    }
    false
}

#[cfg(test)]
mod gcp_auth_validation_tests {
    use super::*;
    use restate_types::deployment::{GoogleIdTokenAuth, HttpAuth};

    fn http_auth() -> HttpAuth {
        HttpAuth::GoogleIdToken(GoogleIdTokenAuth {
            impersonate_service_account: None,
            audience: None,
        })
    }

    fn assert_invalid_field(result: Result<(), MetaApiError>, expected_field: &str) {
        match result {
            Err(MetaApiError::InvalidField(field, _)) => assert_eq!(field, expected_field),
            other => panic!("expected InvalidField({expected_field}), got {other:?}"),
        }
    }

    // REQ-VAL-01: non-https rejected for public hosts; allowed for loopback/private.
    #[test]
    fn req_val_01_rejects_non_https_public_host() {
        let uri: Uri = "http://example.com/".parse().unwrap();
        assert_invalid_field(validate_http_auth(&uri, &http_auth(), None), "auth");
    }

    #[test]
    fn req_val_01_accepts_https_public_host() {
        let uri: Uri = "https://svc.example.com/".parse().unwrap();
        validate_http_auth(&uri, &http_auth(), None).expect("https public host accepted");
    }

    #[test]
    fn req_val_01_accepts_non_https_loopback() {
        for host in ["localhost", "127.0.0.1", "[::1]", "10.0.0.1", "[fc00::1]"] {
            let uri: Uri = format!("http://{host}/").parse().unwrap();
            validate_http_auth(&uri, &http_auth(), None)
                .unwrap_or_else(|e| panic!("expected accept for {host}, got {e:?}"));
        }
    }

    // REQ-VAL-02: reject when additional_headers carries BOTH Authorization
    // AND X-Serverless-Authorization.
    #[test]
    fn req_val_02_rejects_dual_authorization_headers() {
        let uri: Uri = "https://svc.example.com/".parse().unwrap();
        let mut headers_map = std::collections::HashMap::new();
        headers_map.insert(
            http::HeaderName::from_static("authorization"),
            http::HeaderValue::from_static("Bearer x"),
        );
        headers_map.insert(
            http::HeaderName::from_static("x-serverless-authorization"),
            http::HeaderValue::from_static("Bearer y"),
        );
        let serdeable: restate_serde_util::SerdeableHeaderHashMap = headers_map.into();

        assert_invalid_field(
            validate_http_auth(&uri, &http_auth(), Some(&serdeable)),
            "additional_headers",
        );
    }

    #[test]
    fn req_val_02_accepts_single_authorization_header() {
        let uri: Uri = "https://svc.example.com/".parse().unwrap();
        let mut headers_map = std::collections::HashMap::new();
        headers_map.insert(
            http::HeaderName::from_static("authorization"),
            http::HeaderValue::from_static("Bearer x"),
        );
        let serdeable: restate_serde_util::SerdeableHeaderHashMap = headers_map.into();

        validate_http_auth(&uri, &http_auth(), Some(&serdeable))
            .expect("single Authorization header is allowed");
    }
}
