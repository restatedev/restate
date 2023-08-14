use super::error::*;
use super::state::*;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, Uri};
use axum::response::IntoResponse;
use axum::{http, Json};
use okapi_operation::*;
use restate_schema_api::endpoint::{EndpointMetadataResolver, ProtocolType};
use restate_serde_util::SerdeableHeaderHashMap;
use restate_types::identifiers::{EndpointId, ServiceRevision};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::sync::Arc;

#[serde_as]
#[derive(Debug, Deserialize, JsonSchema)]
pub struct RegisterServiceEndpointRequest {
    /// # Uri
    ///
    /// Uri to use to discover/invoke the service endpoint.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[schemars(with = "String")]
    pub uri: Uri,
    /// # Additional headers
    ///
    /// Additional headers added to the discover/invoke requests to the service endpoint.
    pub additional_headers: Option<SerdeableHeaderHashMap>,
    /// # Force
    ///
    /// If `true`, it will override, if existing, any endpoint using the same `uri`.
    /// Beware that this can lead in-flight invocations to an unrecoverable error state.
    ///
    /// By default, this is `true` but it might change in future to `false`.
    ///
    /// See the [versioning documentation](https://docs.restate.dev/services/upgrades-removal) for more information.
    #[serde(default = "RegisterServiceEndpointRequest::default_force")]
    pub force: bool,
}

impl RegisterServiceEndpointRequest {
    fn default_force() -> bool {
        true
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct RegisterServiceResponse {
    name: String,
    revision: ServiceRevision,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct RegisterServiceEndpointResponse {
    id: EndpointId,
    services: Vec<RegisterServiceResponse>,
}

/// Create service endpoint and return discovered services.
#[openapi(
    summary = "Create service endpoint",
    description = "Create service endpoint. Restate will invoke the endpoint to gather additional information required for registration, such as the services exposed by the service endpoint and their Protobuf descriptor. If the service endpoint is already registered, this method will fail unless `force` is set to `true`.",
    operation_id = "create_service_endpoint",
    tags = "service_endpoint",
    responses(
        ignore_return_type = true,
        response(
            status = "201",
            description = "Created",
            content = "Json<RegisterServiceEndpointResponse>",
        ),
        from_type = "MetaApiError",
    )
)]
pub async fn create_service_endpoint<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    #[request_body(required = true)] Json(payload): Json<RegisterServiceEndpointRequest>,
) -> Result<impl IntoResponse, MetaApiError> {
    let registration_result = state
        .meta_handle()
        .register_endpoint(
            payload.uri,
            payload.additional_headers.unwrap_or_default().into(),
            payload.force,
        )
        .await?;

    let response_body = RegisterServiceEndpointResponse {
        id: registration_result.endpoint,
        services: registration_result
            .services
            .into_iter()
            .map(|(name, revision)| RegisterServiceResponse { name, revision })
            .collect(),
    };

    Ok((
        StatusCode::CREATED,
        [(
            http::header::LOCATION,
            format!("/endpoints/{}", response_body.id),
        )],
        Json(response_body),
    ))
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ServiceEndpointResponse {
    id: EndpointId,
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[schemars(with = "String")]
    uri: Uri,
    protocol_type: ProtocolType,
    additional_headers: SerdeableHeaderHashMap,
    /// # Services
    ///
    /// List of services exposed by this service endpoint.
    services: Vec<RegisterServiceResponse>,
}

/// Discover endpoint and return discovered endpoints.
#[openapi(
    summary = "Get service endpoint",
    description = "Get service endpoint metadata",
    operation_id = "get_service_endpoint",
    tags = "service_endpoint",
    parameters(path(
        name = "endpoint",
        description = "Endpoint identifier",
        schema = "std::string::String"
    ))
)]
pub async fn get_service_endpoint<S: EndpointMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(endpoint_id): Path<String>,
) -> Result<Json<ServiceEndpointResponse>, MetaApiError> {
    let (endpoint_meta, services) = state
        .schemas()
        .get_endpoint_and_services(&endpoint_id)
        .ok_or_else(|| MetaApiError::ServiceEndpointNotFound(endpoint_id.clone()))?;

    Ok(ServiceEndpointResponse {
        id: endpoint_id,
        uri: endpoint_meta.address().clone(),
        protocol_type: endpoint_meta.protocol_type(),
        additional_headers: endpoint_meta.additional_headers().clone().into(),
        services: services
            .into_iter()
            .map(|(name, revision)| RegisterServiceResponse { name, revision })
            .collect(),
    }
    .into())
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeleteServiceEndpointParams {
    force: Option<bool>,
}

/// Discover endpoint and return discovered endpoints.
#[openapi(
    summary = "Delete service endpoint",
    description = "Delete service endpoint. Currently it's supported to remove a service endpoint only using the force flag",
    operation_id = "delete_service_endpoint",
    tags = "service_endpoint",
    parameters(
        path(
            name = "endpoint",
            description = "Endpoint identifier",
            schema = "std::string::String"
        ),
        query(
            name = "force",
            description = "If true, the service endpoint will be forcefully deleted. This might break in-flight invocations, use with caution.",
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
pub async fn delete_service_endpoint<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(endpoint_id): Path<String>,
    Query(DeleteServiceEndpointParams { force }): Query<DeleteServiceEndpointParams>,
) -> Result<StatusCode, MetaApiError> {
    if let Some(true) = force {
        state.meta_handle().remove_endpoint(endpoint_id).await?;
        Ok(StatusCode::ACCEPTED)
    } else {
        Ok(StatusCode::NOT_IMPLEMENTED)
    }
}
