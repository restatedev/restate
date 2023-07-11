use super::error::*;
use super::state::*;

use axum::extract::{Path, State};
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
        .register(
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
