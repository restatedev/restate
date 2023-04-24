use super::error::*;
use super::state::*;
use axum::extract::{Path, State};
use axum::Json;
use okapi_operation::*;
use restate_service_metadata::MethodDescriptorRegistry;
use schemars::JsonSchema;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListServiceMethodsResponse {
    methods: Vec<GetServiceMethodResponse>,
}

/// List discovered methods for service
#[openapi(
    summary = "List service methods",
    operation_id = "list_service_methods",
    tags = "service_method",
    parameters(path(name = "service", schema = "std::string::String"))
)]
pub async fn list_service_methods<S, M: MethodDescriptorRegistry>(
    State(state): State<Arc<RestEndpointState<S, M>>>,
    Path(service_name): Path<String>,
) -> Result<Json<ListServiceMethodsResponse>, MetaApiError> {
    match state
        .method_descriptor_registry()
        .list_methods(service_name.as_str())
    {
        Some(methods) => Ok(ListServiceMethodsResponse {
            methods: methods
                .keys()
                .map(|method_name| GetServiceMethodResponse {
                    service_name: service_name.clone(),
                    method_name: method_name.clone(),
                })
                .collect(),
        }
        .into()),
        None => Err(MetaApiError::ServiceNotFound(service_name)),
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetServiceMethodResponse {
    service_name: String,
    method_name: String,
}

/// Get an method of a service
#[openapi(
    summary = "Get service method",
    operation_id = "get_service_method",
    tags = "service_method",
    parameters(
        path(name = "service", schema = "std::string::String"),
        path(name = "method", schema = "std::string::String")
    )
)]
pub async fn get_service_method<S, M: MethodDescriptorRegistry>(
    State(state): State<Arc<RestEndpointState<S, M>>>,
    Path((service_name, method_name)): Path<(String, String)>,
) -> Result<Json<GetServiceMethodResponse>, MetaApiError> {
    let endpoint = state
        .method_descriptor_registry()
        .resolve_method_descriptor(service_name.as_str(), method_name.as_str());
    match endpoint {
        Some(_) => Ok(GetServiceMethodResponse {
            service_name,
            method_name,
        }
        .into()),
        None => Err(MetaApiError::MethodNotFound {
            service_name,
            method_name,
        }),
    }
}
