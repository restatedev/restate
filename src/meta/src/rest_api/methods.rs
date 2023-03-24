use super::error::*;
use super::state::*;
use crate::rest_api::error::MetaApiError::NotFound;
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use service_metadata::MethodDescriptorRegistry;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct ListMethodsRequest {}

#[derive(Debug, Serialize)]
pub struct ListMethodsResponse {
    methods: Vec<GetMethodResponse>,
}

/// List discovered methods for service
pub async fn list_methods<S, M: MethodDescriptorRegistry>(
    State(state): State<Arc<RestEndpointState<S, M>>>,
    Path(service_name): Path<String>,
    Query(_): Query<ListMethodsRequest>,
) -> Result<Json<ListMethodsResponse>, MetaApiError> {
    match state
        .method_descriptor_registry()
        .list_methods(service_name.as_str())
    {
        Some(methods) => Ok(ListMethodsResponse {
            methods: methods
                .keys()
                .map(|method_name| GetMethodResponse {
                    service_name: service_name.clone(),
                    method_name: method_name.clone(),
                })
                .collect(),
        }
        .into()),
        None => Err(NotFound("service", service_name)),
    }
}

#[derive(Debug, Deserialize)]
pub struct GetMethodRequest {}

#[derive(Debug, Serialize)]
pub struct GetMethodResponse {
    service_name: String,
    method_name: String,
}

/// Get an method of a service
pub async fn get_method<S, M: MethodDescriptorRegistry>(
    State(state): State<Arc<RestEndpointState<S, M>>>,
    Path((service_name, method_name)): Path<(String, String)>,
    Query(_): Query<GetMethodRequest>,
) -> Result<Json<GetMethodResponse>, MetaApiError> {
    let endpoint = state
        .method_descriptor_registry()
        .resolve_method_descriptor(service_name.as_str(), method_name.as_str());
    match endpoint {
        Some(_) => Ok(GetMethodResponse {
            service_name,
            method_name,
        }
        .into()),
        None => Err(NotFound(
            "method",
            format!("{}/{}", service_name, method_name),
        )),
    }
}
