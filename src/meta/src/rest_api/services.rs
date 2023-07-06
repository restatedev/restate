use super::error::*;
use super::state::*;

use axum::extract::{Path, State};
use axum::Json;
use okapi_operation::*;
use restate_schema_api::service::{ServiceMetadata, ServiceMetadataResolver};
use schemars::JsonSchema;
use serde::Serialize;
use std::sync::Arc;

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
