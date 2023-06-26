use super::error::*;
use super::state::*;
use axum::extract::{Path, State};
use axum::http::Uri;
use axum::Json;
use hyper::http::{HeaderName, HeaderValue};
use okapi_operation::*;
use restate_common::retry_policy::RetryPolicy;
use restate_service_metadata::{EndpointMetadata, ServiceEndpointRegistry};
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
    /// # Retry policy
    ///
    /// Custom retry policy to use when executing invoke requests to the service endpoint.
    /// If not set, the one configured in the worker will be used as default.
    pub retry_policy: Option<RetryPolicy>,
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
pub async fn discover_service_endpoint<S: ServiceEndpointRegistry, M, K, W>(
    State(state): State<Arc<RestEndpointState<S, M, K, W>>>,
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

    let registration_result = state
        .meta_handle()
        .register(payload.uri, headers, payload.retry_policy)
        .await;
    Ok(registration_result
        .map(|services| RegisterServiceEndpointResponse { services })?
        .into())
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListServicesResponse {
    endpoints: Vec<GetServiceResponse>,
}

/// List services
#[openapi(
    summary = "List services",
    description = "List all registered services.",
    operation_id = "list_services",
    tags = "service"
)]
pub async fn list_services<S: ServiceEndpointRegistry, M, K, W>(
    State(state): State<Arc<RestEndpointState<S, M, K, W>>>,
) -> Result<Json<ListServicesResponse>, MetaApiError> {
    Ok(ListServicesResponse {
        endpoints: state
            .service_endpoint_registry()
            .list_endpoints()
            .iter()
            .map(|(service_name, metadata)| GetServiceResponse {
                service_name: service_name.clone(),
                endpoint_metadata: metadata.clone(),
            })
            .collect(),
    }
    .into())
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct GetServiceResponse {
    service_name: String,
    endpoint_metadata: EndpointMetadata,
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
pub async fn get_service<S: ServiceEndpointRegistry, M, K, W>(
    State(state): State<Arc<RestEndpointState<S, M, K, W>>>,
    Path(service_name): Path<String>,
) -> Result<Json<GetServiceResponse>, MetaApiError> {
    match state
        .service_endpoint_registry()
        .resolve_endpoint(service_name.clone())
    {
        Some(endpoint_metadata) => Ok(GetServiceResponse {
            service_name,
            endpoint_metadata,
        }
        .into()),
        None => Err(MetaApiError::ServiceNotFound(service_name)),
    }
}
