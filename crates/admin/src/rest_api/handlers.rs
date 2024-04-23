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

use crate::state::AdminServiceState;
use axum::extract::{Path, State};
use axum::Json;
use okapi_operation::*;
use restate_meta_rest_model::handlers::*;

/// List discovered handlers for service
#[openapi(
    summary = "List service handlers",
    description = "List all the handlers of the given service.",
    operation_id = "list_service_handlers",
    tags = "service_handler",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    ))
)]
pub async fn list_service_handlers<V>(
    State(state): State<AdminServiceState<V>>,
    Path(service_name): Path<String>,
) -> Result<Json<ListServiceHandlersResponse>, MetaApiError> {
    match state
        .task_center
        .run_in_scope_sync("list-service-handlers", None, || {
            state.schema_registry.list_service_handlers(&service_name)
        }) {
        Some(handlers) => Ok(ListServiceHandlersResponse { handlers }.into()),
        None => Err(MetaApiError::ServiceNotFound(service_name)),
    }
}

/// Get a handler of a service
#[openapi(
    summary = "Get service handler",
    description = "Get the handler of a service",
    operation_id = "get_service_handler",
    tags = "service_handler",
    parameters(
        path(
            name = "service",
            description = "Fully qualified service name.",
            schema = "std::string::String"
        ),
        path(
            name = "handler",
            description = "Handler name.",
            schema = "std::string::String"
        )
    )
)]
pub async fn get_service_handler<V>(
    State(state): State<AdminServiceState<V>>,
    Path((service_name, handler_name)): Path<(String, String)>,
) -> Result<Json<HandlerMetadata>, MetaApiError> {
    match state
        .task_center
        .run_in_scope_sync("get-service-handler", None, || {
            state
                .schema_registry
                .get_service_handler(&service_name, &handler_name)
        }) {
        Some(metadata) => Ok(metadata.into()),
        _ => Err(MetaApiError::HandlerNotFound {
            service_name,
            handler_name,
        }),
    }
}
