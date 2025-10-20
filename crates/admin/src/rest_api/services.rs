// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use tracing::{debug, warn};

use axum::Json;
use axum::extract::{Path, State};
use bytes::Bytes;
use http::StatusCode;
use okapi_operation::*;

use restate_admin_rest_model::services::ListServicesResponse;
use restate_admin_rest_model::services::*;
use restate_core::TaskCenter;
use restate_errors::warn_it;
use restate_types::config::Configuration;
use restate_types::identifiers::{ServiceId, WithPartitionKey};
use restate_types::schema;
use restate_types::schema::registry::MetadataService;
use restate_types::schema::service::ServiceMetadata;
use restate_types::state_mut::ExternalStateMutation;
use restate_wal_protocol::{Command, Envelope};

use super::create_envelope_header;
use super::error::*;
use crate::state::AdminServiceState;

/// List services
#[openapi(
    summary = "List services",
    description = "List all registered services.",
    operation_id = "list_services",
    tags = "service"
)]
pub async fn list_services<Metadata, Discovery, Telemetry, Invocations>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations>>,
) -> Result<Json<ListServicesResponse>, MetaApiError>
where
    Metadata: MetadataService,
{
    let services = state.schema_registry.list_services();

    Ok(ListServicesResponse { services }.into())
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
pub async fn get_service<Metadata, Discovery, Telemetry, Invocations>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations>>,
    Path(service_name): Path<String>,
) -> Result<Json<ServiceMetadata>, MetaApiError>
where
    Metadata: MetadataService,
{
    state
        .schema_registry
        .get_service(&service_name)
        .map(Into::into)
        .ok_or_else(|| MetaApiError::ServiceNotFound(service_name))
}

/// Get service OpenAPI definition
#[openapi(
    summary = "Get service OpenAPI",
    description = "Get the service OpenAPI 3.1 contract.",
    operation_id = "get_service_openapi",
    tags = "service",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    )),
    responses(
        ignore_return_type = true,
        response(
            status = "200",
            description = "OpenAPI 3.1 of the service",
            content = "Json<serde_json::Value>",
        ),
        from_type = "MetaApiError",
    )
)]
pub async fn get_service_openapi<Metadata, Discovery, Telemetry, Invocations>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations>>,
    Path(service_name): Path<String>,
) -> Result<Json<serde_json::Value>, MetaApiError>
where
    Metadata: MetadataService,
{
    // TODO return correct vnd type
    // TODO accept content negotiation for yaml
    let ingress_address = TaskCenter::with_current(|tc| {
        Configuration::pinned()
            .ingress
            .advertised_address(tc.address_book())
    });
    state
        .schema_registry
        .get_service_openapi(&service_name, ingress_address)
        .map(Into::into)
        .ok_or_else(|| MetaApiError::ServiceNotFound(service_name))
}

/// Modify a service
#[openapi(
    summary = "Modify a service",
    description = "Modify a registered service configuration. NOTE: Service re-discovery will update the settings based on the service endpoint configuration.",
    operation_id = "modify_service",
    tags = "service",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    ))
)]
pub async fn modify_service<Metadata, Discovery, Telemetry, Invocations>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations>>,
    Path(service_name): Path<String>,
    #[request_body(required = true)] Json(ModifyServiceRequest {
        public,
        idempotency_retention,
        workflow_completion_retention,
        journal_retention,
        inactivity_timeout,
        abort_timeout,
    }): Json<ModifyServiceRequest>,
) -> Result<Json<ServiceMetadata>, MetaApiError>
where
    Metadata: MetadataService,
{
    let modify_request = schema::registry::ModifyServiceRequest {
        public,
        idempotency_retention,
        journal_retention,
        workflow_completion_retention,
        inactivity_timeout,
        abort_timeout,
    };

    if modify_request.public.is_none()
        && modify_request.idempotency_retention.is_none()
        && modify_request.journal_retention.is_none()
        && modify_request.workflow_completion_retention.is_none()
        && modify_request.inactivity_timeout.is_none()
        && modify_request.abort_timeout.is_none()
    {
        // No need to do anything
        return get_service(State(state), Path(service_name)).await;
    }

    let response = state
        .schema_registry
        .modify_service(service_name, modify_request)
        .await
        .inspect_err(|e| warn_it!(e))?;

    Ok(response.into())
}

/// Modify a service state
#[openapi(
    summary = "Modify a service state",
    description = "Modify service state",
    operation_id = "modify_service_state",
    tags = "service",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    )),
    responses(
        ignore_return_type = true,
        response(
            status = "202",
            description = "Accepted",
            content = "okapi_operation::Empty",
        ),
        from_type = "MetaApiError",
    )
)]
pub async fn modify_service_state<Metadata, Discovery, Telemetry, Invocations>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations>>,
    Path(service_name): Path<String>,
    #[request_body(required = true)] Json(ModifyServiceStateRequest {
        version,
        object_key,
        new_state,
    }): Json<ModifyServiceStateRequest>,
) -> Result<StatusCode, MetaApiError>
where
    Metadata: MetadataService,
{
    if let Some(svc) = state.schema_registry.get_service(&service_name) {
        if !svc.ty.has_state() {
            return Err(MetaApiError::UnsupportedOperation("modify state", svc.ty));
        }
    } else if new_state.is_empty() {
        // could be a deleted service; we still want to allow state to be cleared, so lets continue given that the new state is empty
        debug!(
            rpc.service = service_name,
            "Attempting to delete state for service that does not exist in the registry (perhaps deleted)"
        );
    } else {
        return Err(MetaApiError::ServiceNotFound(service_name));
    }

    let service_id = ServiceId::new(service_name, object_key);

    let new_state = new_state
        .into_iter()
        .map(|(k, v)| (Bytes::from(k), v))
        .collect();

    let partition_key = service_id.partition_key();
    let patch_state = ExternalStateMutation {
        service_id,
        version,
        state: new_state,
    };

    let result = restate_bifrost::append_to_bifrost(
        &state.bifrost,
        Arc::new(Envelope::new(
            create_envelope_header(partition_key),
            Command::PatchState(patch_state),
        )),
    )
    .await;

    if let Err(err) = result {
        warn!("Could not append state patching command to Bifrost: {err}");
        Err(MetaApiError::Internal(
            "Failed sending state patching command to the cluster.".to_owned(),
        ))
    } else {
        Ok(StatusCode::ACCEPTED)
    }
}
