use super::error::*;
use super::state::*;

use axum::extract::{Path, State};
use axum::http::Uri;
use axum::Json;
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
    /// See the [versioning documentation](http://restate.dev/docs/deployment-operations/versioning) for more information.
    #[serde(default)]
    pub force: bool,
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
    let registration_result = state
        .meta_handle()
        .register(
            payload.uri,
            payload.additional_headers.unwrap_or_default().into(),
            payload.force,
        )
        .await?;

    Ok(RegisterServiceEndpointResponse {
        id: registration_result.endpoint,
        services: registration_result
            .services
            .into_iter()
            .map(|(name, revision)| RegisterServiceResponse { name, revision })
            .collect(),
    }
    .into())
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ServiceEndpointResponse {
    endpoint_id: EndpointId,
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[schemars(with = "String")]
    address: Uri,
    protocol_type: ProtocolType,
    additional_headers: SerdeableHeaderHashMap,
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
        endpoint_id,
        address: endpoint_meta.address().clone(),
        protocol_type: endpoint_meta.protocol_type(),
        additional_headers: endpoint_meta.additional_headers().clone().into(),
        services: services
            .into_iter()
            .map(|(name, revision)| RegisterServiceResponse { name, revision })
            .collect(),
    }
    .into())
}
