// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::{debug, warn};

use axum::Json;
use axum::extract::{Path, State};
use bytes::Bytes;
use http::StatusCode;

use restate_admin_rest_model::services::ListServicesResponse;
use restate_admin_rest_model::services::*;
use restate_core::TaskCenter;
use restate_core::network::TransportConnect;
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
///
/// Returns a list of all registered services, including their metadata and configuration.
#[utoipa::path(
    get,
    path = "/services",
    operation_id = "list_services",
    tag = "service",
    responses(
        (status = 200, description = "List of all registered services with their metadata", body = ListServicesResponse),
        MetaApiError
    )
)]
pub async fn list_services<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
) -> Result<Json<ListServicesResponse>, MetaApiError>
where
    Metadata: MetadataService,
{
    let services = state.schema_registry.list_services();

    Ok(ListServicesResponse { services }.into())
}

/// Get service
///
/// Returns detailed metadata about a specific service, including its type, handlers, and configuration settings.
#[utoipa::path(
    get,
    path = "/services/{service}",
    operation_id = "get_service",
    tag = "service",
    params(
        ("service" = String, Path, description = "Fully qualified service name."),
    ),
    responses(
        (status = 200, description = "Service metadata including type, revision, handlers, and configuration", body = ServiceMetadata),
        MetaApiError
    )
)]
pub async fn get_service<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
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
///
/// Returns the OpenAPI 3.1 specification for the service, describing all handlers and their request/response schemas.
#[utoipa::path(
    get,
    path = "/services/{service}/openapi",
    operation_id = "get_service_openapi",
    tag = "service",
    params(
        ("service" = String, Path, description = "Fully qualified service name."),
    ),
    responses(
        (status = 200, description = "OpenAPI 3.1 specification document describing the service's API", body = serde_json::Value),
        MetaApiError
    )
)]
pub async fn get_service_openapi<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
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

/// Modify service configuration
///
/// Updates the configuration of a registered service, such as public visibility, retention policies, and timeout settings.
/// Note: Service re-discovery will update these settings based on the service endpoint configuration.
#[utoipa::path(
    patch,
    path = "/services/{service}",
    operation_id = "modify_service",
    tag = "service",
    params(
        ("service" = String, Path, description = "Fully qualified service name."),
    ),
    responses(
        (status = 200, description = "Service configuration updated successfully", body = ServiceMetadata),
        MetaApiError
    )
)]
pub async fn modify_service<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(service_name): Path<String>,
    Json(ModifyServiceRequest {
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

/// Modify service state
///
/// Modifies the K/V state of a Virtual Object. For a detailed description of this API and how to use it, see the [state documentation](https://docs.restate.dev/operate/invocation#modifying-service-state).
#[utoipa::path(
    post,
    path = "/services/{service}/state",
    operation_id = "modify_service_state",
    tag = "service",
    params(
        ("service" = String, Path, description = "Fully qualified service name."),
    ),
    responses(
        (status = 202, description = "State modification request accepted and will be applied asynchronously"),
        MetaApiError
    )
)]
pub async fn modify_service_state<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(mut state): State<
        AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>,
    >,
    Path(service_name): Path<String>,
    Json(ModifyServiceStateRequest {
        version,
        object_key,
        new_state,
    }): Json<ModifyServiceStateRequest>,
) -> Result<StatusCode, MetaApiError>
where
    Metadata: MetadataService,
    Transport: TransportConnect,
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

    let envelope = Envelope::new(
        create_envelope_header(partition_key),
        Command::PatchState(patch_state),
    );

    let result = state
        .ingestion_client
        .ingest(partition_key, envelope)
        .await
        .map_err(|err| {
            warn!("Could not ingest state patching command: {err}");
            MetaApiError::Internal(
                "Failed sending state patching command to the cluster.".to_owned(),
            )
        })?;

    if let Err(err) = result.await {
        warn!("Could not ingest state patching command: {err}");
        Err(MetaApiError::Internal(
            "Failed sending state patching command to the cluster.".to_owned(),
        ))
    } else {
        Ok(StatusCode::ACCEPTED)
    }
}
