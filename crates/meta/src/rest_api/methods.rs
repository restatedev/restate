// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::error::*;
use super::state::*;

use restate_meta_rest_model::methods::*;
use restate_schema_api::service::ServiceMetadataResolver;

use axum::extract::{Path, State};
use axum::Json;
use okapi_operation::*;

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
) -> Result<Json<MethodMetadata>, MetaApiError> {
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
                Some(method) => Ok(method.into()),
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
