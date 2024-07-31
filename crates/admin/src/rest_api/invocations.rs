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

use crate::rest_api::create_envelope_header;
use crate::state::AdminServiceState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use okapi_operation::*;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::{InvocationTermination, PurgeInvocationRequest};
use restate_wal_protocol::{append_envelope_to_bifrost, Command, Envelope};
use serde::Deserialize;
use tracing::warn;

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub enum DeletionMode {
    #[default]
    #[serde(alias = "cancel")]
    Cancel,
    #[serde(alias = "kill")]
    Kill,
    #[serde(alias = "purge")]
    Purge,
}
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct DeleteInvocationParams {
    pub mode: Option<DeletionMode>,
}

/// Terminate an invocation
#[openapi(
    summary = "Delete an invocation",
    description = "Delete the given invocation. By default, an invocation is terminated by gracefully \
    cancelling it. This ensures virtual object state consistency. Alternatively, an invocation can be killed which \
    does not guarantee consistency for virtual object instance state, in-flight invocations to other services, etc. \
    A stored completed invocation can also be purged",
    operation_id = "delete_invocation",
    tags = "invocation",
    parameters(
        path(
            name = "invocation_id",
            description = "Invocation identifier.",
            schema = "std::string::String"
        ),
        query(
            name = "mode",
            description = "If cancel, it will gracefully terminate the invocation. \
            If kill, it will terminate the invocation with a hard stop. \
            If purge, it will only cleanup the response for completed invocations, and leave unaffected an in-flight invocation.",
            required = false,
            style = "simple",
            allow_empty_value = false,
            schema = "DeletionMode",
        )
    ),
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
pub async fn delete_invocation<V>(
    State(state): State<AdminServiceState<V>>,
    Path(invocation_id): Path<String>,
    Query(DeleteInvocationParams { mode }): Query<DeleteInvocationParams>,
) -> Result<StatusCode, MetaApiError> {
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| MetaApiError::InvalidField("invocation_id", e.to_string()))?;

    let cmd = match mode.unwrap_or_default() {
        DeletionMode::Cancel => {
            Command::TerminateInvocation(InvocationTermination::cancel(invocation_id))
        }
        DeletionMode::Kill => {
            Command::TerminateInvocation(InvocationTermination::kill(invocation_id))
        }
        DeletionMode::Purge => Command::PurgeInvocation(PurgeInvocationRequest { invocation_id }),
    };

    let partition_key = invocation_id.partition_key();

    let result = state
        .task_center
        .run_in_scope(
            "delete_invocation",
            None,
            append_envelope_to_bifrost(
                &state.bifrost,
                Envelope::new(create_envelope_header(partition_key), cmd),
            ),
        )
        .await;

    if let Err(err) = result {
        warn!("Could not append invocation termination command to Bifrost: {err}");
        Err(MetaApiError::Internal(
            "Failed sending invocation termination to the cluster.".to_owned(),
        ))
    } else {
        Ok(StatusCode::ACCEPTED)
    }
}
