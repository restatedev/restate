use super::error::*;
use super::state::*;
use axum::extract::{Path, State};
use axum::http::Uri;
use axum::Json;
use hyper::http::{HeaderName, HeaderValue};
use restate_service_metadata::{EndpointMetadata, ServiceEndpointRegistry};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;
use std::sync::Arc;

#[serde_as]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterServiceEndpointRequest {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub uri: Uri,
    pub additional_headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterServiceEndpointResponse {
    services: Vec<String>,
}

/// Discover endpoint and return discovered endpoints.
pub async fn discover_service_endpoint<S: ServiceEndpointRegistry, M>(
    State(state): State<Arc<RestEndpointState<S, M>>>,
    Json(payload): Json<RegisterServiceEndpointRequest>,
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListServicesResponse {
    endpoints: Vec<GetServiceResponse>,
}

/// List services
pub async fn list_services<S: ServiceEndpointRegistry, M>(
    State(state): State<Arc<RestEndpointState<S, M>>>,
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetServiceResponse {
    service_name: String,
    endpoint_metadata: EndpointMetadata,
}

/// Get a service
pub async fn get_service<S: ServiceEndpointRegistry, M>(
    State(state): State<Arc<RestEndpointState<S, M>>>,
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
