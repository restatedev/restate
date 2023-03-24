use super::error::*;
use super::state::*;
use axum::extract::{Path, Query, State};
use axum::http::Uri;
use axum::Json;
use hyper::http::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use service_metadata::{EndpointMetadata, ServiceEndpointRegistry};
use std::collections::HashMap;
use std::sync::Arc;

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct RegisterEndpointRequest {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub uri: Uri,
    pub additional_headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub struct RegisterEndpointResponse {
    services: Vec<String>,
}

/// Discover endpoint and return discovered endpoints.
pub async fn discover_endpoint<S: ServiceEndpointRegistry, M>(
    State(state): State<Arc<RestEndpointState<S, M>>>,
    Json(payload): Json<RegisterEndpointRequest>,
) -> Result<Json<RegisterEndpointResponse>, MetaApiError> {
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
        .collect::<Result<HashMap<HeaderName, HeaderValue>, MetaApiError>>()?;

    let registration_result = state.meta_handle().register(payload.uri, headers).await;
    Ok(registration_result
        .map(|services| RegisterEndpointResponse { services })?
        .into())
}

#[derive(Debug, Deserialize)]
pub struct ListEndpointsRequest {}

#[derive(Debug, Serialize)]
pub struct ListEndpointsResponse {
    endpoints: Vec<GetEndpointResponse>,
}

/// List discovered endpoints
pub async fn list_endpoints<S: ServiceEndpointRegistry, M>(
    State(state): State<Arc<RestEndpointState<S, M>>>,
    Query(_): Query<ListEndpointsRequest>,
) -> Result<Json<ListEndpointsResponse>, MetaApiError> {
    Ok(ListEndpointsResponse {
        endpoints: state
            .service_endpoint_registry()
            .list_endpoints()
            .iter()
            .map(|(service_name, metadata)| GetEndpointResponse {
                service_name: service_name.clone(),
                metadata: metadata.clone(),
            })
            .collect(),
    }
    .into())
}

#[derive(Debug, Deserialize)]
pub struct GetEndpointRequest {}

#[derive(Debug, Serialize)]
pub struct GetEndpointResponse {
    service_name: String,
    metadata: EndpointMetadata,
}

/// Get an endpoint
pub async fn get_endpoint<S: ServiceEndpointRegistry, M>(
    State(state): State<Arc<RestEndpointState<S, M>>>,
    Path(service_name): Path<String>,
    Query(_): Query<GetEndpointRequest>,
) -> Result<Json<GetEndpointResponse>, MetaApiError> {
    let endpoint = state
        .service_endpoint_registry()
        .resolve_endpoint(service_name.clone());
    match endpoint {
        Some(metadata) => Ok(GetEndpointResponse {
            service_name,
            metadata,
        }
        .into()),
        None => Err(MetaApiError::NotFound("service", service_name)),
    }
}
