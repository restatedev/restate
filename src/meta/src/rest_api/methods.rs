use super::error::*;
use super::state::*;
use axum::extract::{Path, State};
use axum::Json;
use restate_service_metadata::MethodDescriptorRegistry;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct ListServiceMethodsResponse {
    methods: Vec<GetServiceMethodResponse>,
}

/// List discovered methods for service
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

#[derive(Debug, Serialize)]
pub struct GetServiceMethodResponse {
    service_name: String,
    method_name: String,
}

/// Get an method of a service
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
