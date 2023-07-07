use super::error::*;
use super::state::*;
use axum::extract::{Path, State};
use axum::Json;
use okapi_operation::*;
use restate_schema_api::service::ServiceMetadataResolver;
use schemars::JsonSchema;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListServiceMethodsResponse {
    methods: Vec<String>,
}

/// List discovered methods for service
#[openapi(
    summary = "List service methods",
    description = "List all the methods of the given service.",
    operation_id = "list_service_methods",
    tags = "service_method",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    ))
)]
pub async fn list_service_methods<S: ServiceMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(service_name): Path<String>,
) -> Result<Json<ListServiceMethodsResponse>, MetaApiError> {
    match state
        .schemas()
        .resolve_latest_service_metadata(&service_name)
    {
        Some(metadata) => Ok(ListServiceMethodsResponse {
            methods: metadata.methods,
        }
        .into()),
        None => Err(MetaApiError::ServiceNotFound(service_name)),
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct GetServiceMethodResponse {
    service_name: String,
    method_name: String,
}

/// Get an method of a service
#[openapi(
    summary = "Get service method",
    description = "Get the method of a service",
    operation_id = "get_service_method",
    tags = "service_method",
    parameters(
        path(
            name = "service",
            description = "Fully qualified service name.",
            schema = "std::string::String"
        ),
        path(
            name = "method",
            description = "Method name.",
            schema = "std::string::String"
        )
    )
)]
pub async fn get_service_method<S: ServiceMetadataResolver, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path((service_name, method_name)): Path<(String, String)>,
) -> Result<Json<GetServiceMethodResponse>, MetaApiError> {
    match state
        .schemas()
        .resolve_latest_service_metadata(&service_name)
    {
        Some(metadata) if metadata.methods.contains(&method_name) => Ok(GetServiceMethodResponse {
            service_name,
            method_name,
        }
        .into()),
        _ => Err(MetaApiError::MethodNotFound {
            service_name,
            method_name,
        }),
    }
}
