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
use futures::TryFutureExt;
use okapi_operation::*;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::InvocationTermination;
use restate_wal_protocol::{append_envelope_to, Command, Envelope};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub enum TerminationMode {
    #[default]
    #[serde(alias = "cancel")]
    Cancel,
    #[serde(alias = "kill")]
    Kill,
}
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct DeleteInvocationParams {
    pub mode: Option<TerminationMode>,
}

/// Terminate an invocation
#[openapi(
    summary = "Terminate an invocation",
    description = "Terminate the given invocation. By default, an invocation is terminated by gracefully \
    cancelling it. This ensures service state consistency. Alternatively, an invocation can be killed which \
    does not guarantee consistency for service instance state, in-flight invocation to other services, etc.",
    operation_id = "terminate_invocation",
    tags = "invocation",
    parameters(
        path(
            name = "invocation_id",
            description = "Invocation identifier.",
            schema = "std::string::String"
        ),
        query(
            name = "mode",
            description = "If cancel, it will gracefully terminate the invocation. If kill, it will terminate the invocation with a hard stop.",
            required = false,
            style = "simple",
            allow_empty_value = false,
            schema = "TerminationMode",
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
pub async fn delete_invocation(
    State(mut state): State<AdminServiceState>,
    Path(invocation_id): Path<String>,
    Query(DeleteInvocationParams { mode }): Query<DeleteInvocationParams>,
) -> Result<StatusCode, MetaApiError> {
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| MetaApiError::InvalidField("invocation_id", e.to_string()))?;

    let invocation_termination = match mode.unwrap_or_default() {
        TerminationMode::Cancel => InvocationTermination::cancel(invocation_id),
        TerminationMode::Kill => InvocationTermination::kill(invocation_id),
    };

    let partition_key = invocation_termination.maybe_fid.partition_key();

    state
        .task_center
        .run_in_scope(
            "delete_invocation",
            None,
            append_envelope_to(
                &mut state.bifrost,
                Envelope::new(
                    create_envelope_header(partition_key),
                    Command::TerminateInvocation(invocation_termination),
                ),
            )
            .map_err(MetaApiError::Generic),
        )
        .await?;

    Ok(StatusCode::ACCEPTED)
}
