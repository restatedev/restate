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
use super::{create_envelope_header, notify_node_about_schema_changes};
use crate::state::AdminServiceState;

use axum::extract::{Path, State};
use axum::Json;
use bytes::Bytes;
use http::StatusCode;
use okapi_operation::*;
use restate_meta_rest_model::components::ListComponentsResponse;
use restate_meta_rest_model::components::*;
use restate_schema_api::component::ComponentMetadataResolver;
use restate_types::identifiers::{ServiceId, WithPartitionKey};
use restate_types::state_mut::ExternalStateMutation;
use restate_wal_protocol::{append_envelope_to_bifrost, Command, Envelope};
use tracing::warn;

/// List components
#[openapi(
    summary = "List components",
    description = "List all registered components.",
    operation_id = "list_components",
    tags = "component"
)]
pub async fn list_components(
    State(state): State<AdminServiceState>,
) -> Result<Json<ListComponentsResponse>, MetaApiError> {
    Ok(ListComponentsResponse {
        components: state.schemas().list_components(),
    }
    .into())
}

/// Get a component
#[openapi(
    summary = "Get component",
    description = "Get a registered component.",
    operation_id = "get_component",
    tags = "component",
    parameters(path(
        name = "component",
        description = "Fully qualified component name.",
        schema = "std::string::String"
    ))
)]
pub async fn get_component(
    State(state): State<AdminServiceState>,
    Path(component_name): Path<String>,
) -> Result<Json<ComponentMetadata>, MetaApiError> {
    state
        .schemas()
        .resolve_latest_component(&component_name)
        .map(Into::into)
        .ok_or_else(|| MetaApiError::ComponentNotFound(component_name))
}

/// Modify a component
#[openapi(
    summary = "Modify a component",
    description = "Modify a registered component.",
    operation_id = "modify_component",
    tags = "component",
    parameters(path(
        name = "component",
        description = "Fully qualified component name.",
        schema = "std::string::String"
    ))
)]
pub async fn modify_component(
    State(state): State<AdminServiceState>,
    Path(component_name): Path<String>,
    #[request_body(required = true)] Json(ModifyComponentRequest { public }): Json<
        ModifyComponentRequest,
    >,
) -> Result<Json<ComponentMetadata>, MetaApiError> {
    state
        .meta_handle()
        .modify_component(component_name.clone(), public)
        .await?;

    notify_node_about_schema_changes(state.schema_reader(), state.node_svc_client()).await;

    state
        .schemas()
        .resolve_latest_component(&component_name)
        .map(Into::into)
        .ok_or_else(|| MetaApiError::ComponentNotFound(component_name))
}

/// Modify a component state
#[openapi(
    summary = "Modify a component state",
    description = "Modify component state",
    operation_id = "modify_component_state",
    tags = "component",
    parameters(path(
        name = "component",
        description = "Fully qualified component name.",
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
pub async fn modify_component_state(
    State(mut state): State<AdminServiceState>,
    Path(component_name): Path<String>,
    #[request_body(required = true)] Json(ModifyComponentStateRequest {
        version,
        object_key,
        new_state,
    }): Json<ModifyComponentStateRequest>,
) -> Result<StatusCode, MetaApiError> {
    let component_id = ServiceId::new(component_name, object_key);

    let new_state = new_state
        .into_iter()
        .map(|(k, v)| (Bytes::from(k), v))
        .collect();

    let partition_key = component_id.partition_key();
    let patch_state = ExternalStateMutation {
        component_id,
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
