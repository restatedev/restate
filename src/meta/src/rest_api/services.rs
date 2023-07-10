use super::error::*;
use super::state::*;
use axum::extract::{Path, State};
use axum::http::Uri;
use axum::Json;
use hyper::http::{HeaderName, HeaderValue};
use okapi_operation::*;
use restate_schema_api::service::{ServiceMetadata, ServiceMetadataResolver};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;
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
    pub additional_headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct RegisterServiceEndpointResponse {
    services: Vec<String>,
}

/// Discover endpoint and return discovered endpoints.
#[openapi(
    summary = "Discover service endpoint",
    description = "Discover service endpoint and register it in the meta information storage. If the service endpoint is already registered, it will be re-discovered and will override the previous stored metadata.",
    operation_id = "discover_service_endpoint",
    tags = "service_endpoint"
)]
pub async fn discover_service_endpoint<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    #[request_body(required = true)] Json(payload): Json<RegisterServiceEndpointRequest>,
) -> Result<Json<RegisterServiceEndpointResponse>, MetaApiError> {
    let headers = payload
        .additional_headers
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| {
            let header_name = HeaderName::try_from(k)
                .map_err(|e| MetaApiError::InvalidField("additional_headers", e.to_string()))?;
            let header_value = HeaderValue::try_from(v)
                .map_err(|e| MetaApiError::InvalidField("additional_headers", e.to_string()))?;
            Ok((header_name, header_value))
        })
        .collect::<Result<HashMap<_, _>, MetaApiError>>()?;

    let registration_result = state.meta_handle().register(payload.uri, headers).await;
    Ok(registration_result
        .map(|services| RegisterServiceEndpointResponse { services })?
        .into())
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListServicesResponse {
    services: Vec<ServiceMetadata>,
}

/// List services
#[openapi(
    summary = "List services",
    description = "List all registered services.",
    operation_id = "list_services",
    tags = "service"
)]
pub async fn list_services<S: ServiceMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
) -> Result<Json<ListServicesResponse>, MetaApiError> {
    Ok(ListServicesResponse {
        services: state.schemas().list_services(),
    }
    .into())
}

/// Get a service
#[openapi(
    summary = "Get service",
    description = "Get a registered service.",
    operation_id = "get_service",
    tags = "service",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    ))
)]
pub async fn get_service<S: ServiceMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(service_name): Path<String>,
) -> Result<Json<ServiceMetadata>, MetaApiError> {
    state
        .schemas()
        .resolve_latest_service_metadata(&service_name)
        .map(Into::into)
        .ok_or_else(|| MetaApiError::ServiceNotFound(service_name))
}
