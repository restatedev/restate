// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
    methods: Vec<GetServiceMethodResponse>,
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
            methods: metadata
                .methods
                .into_iter()
                .map(|method| GetServiceMethodResponse {
                    service_name: service_name.clone(),
                    method_name: method.name,
                    input_type: method.input_type,
                    output_type: method.output_type,
                })
                .collect(),
        }
        .into()),
        None => Err(MetaApiError::ServiceNotFound(service_name)),
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct GetServiceMethodResponse {
    service_name: String,
    method_name: String,
    input_type: String,
    output_type: String,
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
        Some(metadata) => {
            match metadata
                .methods
                .into_iter()
                .find(|method| method.name == method_name)
            {
                Some(method) => Ok(GetServiceMethodResponse {
                    service_name,
                    method_name,
                    input_type: method.input_type,
                    output_type: method.output_type,
                }
                .into()),
                _ => Err(MetaApiError::MethodNotFound {
                    service_name,
                    method_name,
                }),
            }
        }
        _ => Err(MetaApiError::MethodNotFound {
            service_name,
            method_name,
        }),
    }
}
