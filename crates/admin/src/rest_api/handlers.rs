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

/// List discovered handlers for component
#[openapi(
    summary = "List component handlers",
    description = "List all the handlers of the given component.",
    operation_id = "list_component_handlers",
    tags = "component_handler",
    parameters(path(
        name = "component",
        description = "Fully qualified component name.",
        schema = "std::string::String"
    ))
)]
pub async fn list_component_handlers<V>(
    State(state): State<AdminServiceState<V>>,
    Path(component_name): Path<String>,
) -> Result<Json<ListComponentHandlersResponse>, MetaApiError> {
    match state
        .task_center
        .run_in_scope_sync("list-component-handlers", None, || {
            state
                .schema_registry
                .list_component_handlers(&component_name)
        }) {
        Some(handlers) => Ok(ListComponentHandlersResponse { handlers }.into()),
        None => Err(MetaApiError::ComponentNotFound(component_name)),
    }
}

/// Get a handler of a component
#[openapi(
    summary = "Get component handler",
    description = "Get the handler of a component",
    operation_id = "get_component_handler",
    tags = "component_handler",
    parameters(
        path(
            name = "component",
            description = "Fully qualified component name.",
            schema = "std::string::String"
        ),
        path(
            name = "handler",
            description = "Handler name.",
            schema = "std::string::String"
        )
    )
)]
pub async fn get_component_handler<V>(
    State(state): State<AdminServiceState<V>>,
    Path((component_name, handler_name)): Path<(String, String)>,
) -> Result<Json<HandlerMetadata>, MetaApiError> {
    match state
        .task_center
        .run_in_scope_sync("get-component-handler", None, || {
            state
                .schema_registry
                .get_component_handler(&component_name, &handler_name)
        }) {
        Some(metadata) => Ok(metadata.into()),
        _ => Err(MetaApiError::HandlerNotFound {
            component_name,
            handler_name,
        }),
    }
}
