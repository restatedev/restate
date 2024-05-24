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
use super::{create_envelope_header, log_error};
use crate::schema_registry::ModifyServiceChange;
use crate::state::AdminServiceState;

use axum::extract::{Path, State};
use axum::Json;
use bytes::Bytes;
use http::StatusCode;
use okapi_operation::*;
use restate_admin_rest_model::services::ListServicesResponse;
use restate_admin_rest_model::services::*;
use restate_types::identifiers::{ServiceId, WithPartitionKey};
use restate_types::state_mut::ExternalStateMutation;
use restate_wal_protocol::{append_envelope_to_bifrost, Command, Envelope};
use tracing::warn;

/// List services
#[openapi(
    summary = "List services",
    description = "List all registered services.",
    operation_id = "list_services",
    tags = "service"
)]
pub async fn list_services<V>(
    State(state): State<AdminServiceState<V>>,
) -> Result<Json<ListServicesResponse>, MetaApiError> {
    let services = state
        .task_center
        .run_in_scope_sync("list-services", None, || {
            state.schema_registry.list_services()
        });

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
pub async fn get_service<V>(
    State(state): State<AdminServiceState<V>>,
    Path(service_name): Path<String>,
) -> Result<Json<ServiceMetadata>, MetaApiError> {
    state
        .task_center
        .run_in_scope_sync("get-service", None, || {
            state.schema_registry.get_service(&service_name)
        })
        .map(Into::into)
        .ok_or_else(|| MetaApiError::ServiceNotFound(service_name))
}

/// Modify a service
#[openapi(
    summary = "Modify a service",
    description = "Modify a registered service.",
    operation_id = "modify_service",
    tags = "service",
    parameters(path(
        name = "service",
        description = "Fully qualified service name.",
        schema = "std::string::String"
    ))
)]
pub async fn modify_service<V>(
    State(state): State<AdminServiceState<V>>,
    Path(service_name): Path<String>,
    #[request_body(required = true)] Json(ModifyServiceRequest {
        public,
        idempotency_retention,
        workflow_completion_retention,
    }): Json<ModifyServiceRequest>,
) -> Result<Json<ServiceMetadata>, MetaApiError> {
    let mut modify_request = vec![];
    if let Some(new_public_value) = public {
        modify_request.push(ModifyServiceChange::Public(new_public_value));
    }
    if let Some(new_idempotency_retention) = idempotency_retention {
        modify_request.push(ModifyServiceChange::IdempotencyRetention(
            new_idempotency_retention,
        ));
    }
    if let Some(new_workflow_completion_retention) = workflow_completion_retention {
        modify_request.push(ModifyServiceChange::WorkflowCompletionRetention(
            new_workflow_completion_retention,
        ));
    }

    if modify_request.is_empty() {
        // No need to do anything
        return get_service(State(state), Path(service_name)).await;
    }

    let response = state
        .task_center
        .run_in_scope("modify-service", None, async {
            log_error(
                state
                    .schema_registry
                    .modify_service(service_name, modify_request)
                    .await,
            )
        })
        .await?;

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
pub async fn modify_service_state<V>(
    State(mut state): State<AdminServiceState<V>>,
    Path(service_name): Path<String>,
    #[request_body(required = true)] Json(ModifyServiceStateRequest {
        version,
        object_key,
        new_state,
    }): Json<ModifyServiceStateRequest>,
) -> Result<StatusCode, MetaApiError> {
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

    let result = state
        .task_center
        .run_in_scope(
            "modify_service_state",
            None,
            append_envelope_to_bifrost(
                &mut state.bifrost,
                Envelope::new(
                    create_envelope_header(partition_key),
                    Command::PatchState(patch_state),
                ),
            ),
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
