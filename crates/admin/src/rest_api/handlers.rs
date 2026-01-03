// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use axum::Json;
use axum::extract::{Path, State};
use restate_admin_rest_model::handlers::*;
use restate_types::schema::registry::MetadataService;
use restate_types::schema::service::HandlerMetadata;

/// List service handlers
///
/// Returns a list of all handlers (methods) available in the specified service.
#[utoipa::path(
    get,
    path = "/services/{service}/handlers",
    operation_id = "list_service_handlers",
    tag = "service_handler",
    params(
        ("service" = String, Path, description = "Fully qualified service name."),
    ),
    responses(
        (status = 200, description = "List of handlers available in the service", body = ListServiceHandlersResponse),
        MetaApiError
    )
)]
pub async fn list_service_handlers<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(service_name): Path<String>,
) -> Result<Json<ListServiceHandlersResponse>, MetaApiError>
where
    Metadata: MetadataService,
{
    match state.schema_registry.list_service_handlers(&service_name) {
        Some(handlers) => Ok(ListServiceHandlersResponse { handlers }.into()),
        None => Err(MetaApiError::ServiceNotFound(service_name)),
    }
}

/// Get service handler
///
/// Returns detailed metadata about a specific handler within a service, including its input/output types and handler type.
#[utoipa::path(
    get,
    path = "/services/{service}/handlers/{handler}",
    operation_id = "get_service_handler",
    tag = "service_handler",
    params(
        ("service" = String, Path, description = "Fully qualified service name."),
        ("handler" = String, Path, description = "Handler name."),
    ),
    responses(
        (status = 200, description = "Handler metadata including input/output types and configuration", body = HandlerMetadata),
        MetaApiError
    )
)]
pub async fn get_service_handler<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path((service_name, handler_name)): Path<(String, String)>,
) -> Result<Json<HandlerMetadata>, MetaApiError>
where
    Metadata: MetadataService,
{
    match state
        .schema_registry
        .get_service_handler(&service_name, &handler_name)
    {
        Some(metadata) => Ok(metadata.into()),
        _ => Err(MetaApiError::HandlerNotFound {
            service_name,
            handler_name,
        }),
    }
}
