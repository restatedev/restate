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
use crate::generate_meta_api_error;
use crate::rest_api::create_envelope_header;
use crate::state::AdminServiceState;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use okapi_operation::*;
use restate_admin_rest_model::invocations::RestartAsNewInvocationResponse;
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId, WithPartitionKey};
use restate_types::invocation::client::{
    self, CancelInvocationResponse, InvocationClient, KillInvocationResponse,
    PurgeInvocationResponse,
};
use restate_types::invocation::{InvocationTermination, PurgeInvocationRequest, TerminationFlavor};
use restate_wal_protocol::{Command, Envelope};
use serde::Deserialize;
use std::sync::Arc;
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
    deprecated = true,
    description = "Use kill_invocation/cancel_invocation/purge_invocation instead.",
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
pub async fn delete_invocation<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(invocation_id): Path<String>,
    Query(DeleteInvocationParams { mode }): Query<DeleteInvocationParams>,
) -> Result<StatusCode, MetaApiError> {
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| MetaApiError::InvalidField("invocation_id", e.to_string()))?;

    let cmd = match mode.unwrap_or_default() {
        DeletionMode::Cancel => Command::TerminateInvocation(InvocationTermination {
            invocation_id,
            flavor: TerminationFlavor::Cancel,
            response_sink: None,
        }),
        DeletionMode::Kill => Command::TerminateInvocation(InvocationTermination {
            invocation_id,
            flavor: TerminationFlavor::Kill,
            response_sink: None,
        }),
        DeletionMode::Purge => Command::PurgeInvocation(PurgeInvocationRequest {
            invocation_id,
            response_sink: None,
        }),
    };

    let partition_key = invocation_id.partition_key();

    let result = restate_bifrost::append_to_bifrost(
        &state.bifrost,
        Arc::new(Envelope::new(create_envelope_header(partition_key), cmd)),
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

generate_meta_api_error!(KillInvocationError: [InvocationNotFoundError, InvocationClientError, InvalidFieldError, InvocationWasAlreadyCompletedError]);

/// Kill an invocation
#[openapi(
    summary = "Kill an invocation",
    description = "Kill the given invocation. \
    This does not guarantee consistency for virtual object instance state, in-flight invocations to other services, etc.",
    operation_id = "kill_invocation",
    tags = "invocation",
    parameters(path(
        name = "invocation_id",
        description = "Invocation identifier.",
        schema = "std::string::String"
    ))
)]
pub async fn kill_invocation<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(invocation_id): Path<String>,
) -> Result<(), KillInvocationError>
where
    IC: InvocationClient,
{
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| InvalidFieldError("invocation_id", e.to_string()))?;

    match state
        .invocation_client
        .kill_invocation(PartitionProcessorRpcRequestId::new(), invocation_id)
        .await
        .map_err(InvocationClientError)?
    {
        KillInvocationResponse::Ok => {}
        KillInvocationResponse::NotFound => {
            Err(InvocationNotFoundError(invocation_id.to_string()))?
        }
        KillInvocationResponse::AlreadyCompleted => Err(InvocationWasAlreadyCompletedError(
            invocation_id.to_string(),
        ))?,
    };

    Ok(())
}

generate_meta_api_error!(CancelInvocationError: [InvocationNotFoundError, InvocationClientError, InvalidFieldError, InvocationWasAlreadyCompletedError]);

/// Cancel an invocation
#[openapi(
    summary = "Cancel an invocation",
    description = "Cancel the given invocation. \
    Canceling an invocation allows it to free any resources it is holding and roll back any changes it has made so far, running compensation code. \
    For more details, checkout https://docs.restate.dev/guides/sagas",
    operation_id = "cancel_invocation",
    tags = "invocation",
    external_docs(url = "https://docs.restate.dev/guides/sagas"),
    parameters(path(
        name = "invocation_id",
        description = "Invocation identifier.",
        schema = "std::string::String"
    )),
    responses(
        ignore_return_type = true,
        response(
            status = "200",
            description = "The invocation has been cancelled.",
            content = "okapi_operation::Empty",
        ),
        response(
            status = "202",
            description = "The cancellation signal was appended to the journal and will be processed by the SDK.",
            content = "okapi_operation::Empty",
        ),
        from_type = "CancelInvocationError",
    )
)]
pub async fn cancel_invocation<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(invocation_id): Path<String>,
) -> Result<StatusCode, CancelInvocationError>
where
    IC: InvocationClient,
{
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| InvalidFieldError("invocation_id", e.to_string()))?;

    match state
        .invocation_client
        .cancel_invocation(PartitionProcessorRpcRequestId::new(), invocation_id)
        .await
        .map_err(InvocationClientError)?
    {
        CancelInvocationResponse::Done => Ok(StatusCode::OK),
        CancelInvocationResponse::Appended => Ok(StatusCode::ACCEPTED),
        CancelInvocationResponse::NotFound => {
            Err(InvocationNotFoundError(invocation_id.to_string()))?
        }
        CancelInvocationResponse::AlreadyCompleted => Err(InvocationWasAlreadyCompletedError(
            invocation_id.to_string(),
        ))?,
    }
}

generate_meta_api_error!(PurgeInvocationError: [InvocationNotFoundError, InvocationClientError, InvalidFieldError, PurgeInvocationNotCompletedError]);

/// Purge an invocation
#[openapi(
    summary = "Purge an invocation",
    description = "Purge the given invocation. This cleanups all the state for the given invocation. This command applies only to completed invocations.",
    operation_id = "purge_invocation",
    tags = "invocation",
    parameters(path(
        name = "invocation_id",
        description = "Invocation identifier.",
        schema = "std::string::String"
    ))
)]
pub async fn purge_invocation<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(invocation_id): Path<String>,
) -> Result<(), PurgeInvocationError>
where
    IC: InvocationClient,
{
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| InvalidFieldError("invocation_id", e.to_string()))?;

    match state
        .invocation_client
        .purge_invocation(PartitionProcessorRpcRequestId::new(), invocation_id)
        .await
        .map_err(InvocationClientError)?
    {
        PurgeInvocationResponse::Ok => {}
        PurgeInvocationResponse::NotFound => {
            Err(InvocationNotFoundError(invocation_id.to_string()))?
        }
        PurgeInvocationResponse::NotCompleted => {
            Err(PurgeInvocationNotCompletedError(invocation_id.to_string()))?
        }
    };

    Ok(())
}

generate_meta_api_error!(PurgeJournalError: [InvocationNotFoundError, InvocationClientError, InvalidFieldError, PurgeInvocationNotCompletedError]);

/// Purge an invocation
#[openapi(
    summary = "Purge an invocation journal",
    description = "Purge the given invocation journal. This cleanups only the journal for the given invocation, retaining the metadata. This command applies only to completed invocations.",
    operation_id = "purge_journal",
    tags = "invocation",
    parameters(path(
        name = "invocation_id",
        description = "Invocation identifier.",
        schema = "std::string::String"
    ))
)]
pub async fn purge_journal<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(invocation_id): Path<String>,
) -> Result<(), PurgeJournalError>
where
    IC: InvocationClient,
{
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| InvalidFieldError("invocation_id", e.to_string()))?;

    match state
        .invocation_client
        .purge_journal(PartitionProcessorRpcRequestId::new(), invocation_id)
        .await
        .map_err(InvocationClientError)?
    {
        PurgeInvocationResponse::Ok => {}
        PurgeInvocationResponse::NotFound => {
            Err(InvocationNotFoundError(invocation_id.to_string()))?
        }
        PurgeInvocationResponse::NotCompleted => {
            Err(PurgeInvocationNotCompletedError(invocation_id.to_string()))?
        }
    };

    Ok(())
}

generate_meta_api_error!(RestartInvocationError: [
    InvocationNotFoundError,
    InvocationClientError,
    InvalidFieldError,
    RestartAsNewInvocationStillRunningError,
    RestartAsNewInvocationUnsupportedError,
    RestartAsNewInvocationMissingInputError,
    RestartAsNewInvocationNotStartedError
]);

/// Restart an invocation
#[openapi(
    summary = "Restart as new invocation",
    description = "Restart the given invocation as new. This will restart the invocation, given its input is available, as a new invocation with a different invocation id.",
    operation_id = "restart_as_new_invocation",
    tags = "invocation",
    parameters(path(
        name = "invocation_id",
        description = "Invocation identifier.",
        schema = "std::string::String"
    ))
)]
pub async fn restart_as_new_invocation<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(invocation_id): Path<String>,
) -> Result<Json<RestartAsNewInvocationResponse>, RestartInvocationError>
where
    IC: InvocationClient,
{
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| InvalidFieldError("invocation_id", e.to_string()))?;

    match state
        .invocation_client
        .restart_as_new_invocation(PartitionProcessorRpcRequestId::new(), invocation_id)
        .await
        .map_err(InvocationClientError)?
    {
        client::RestartAsNewInvocationResponse::Ok { new_invocation_id } => {
            Ok(RestartAsNewInvocationResponse { new_invocation_id }.into())
        }
        client::RestartAsNewInvocationResponse::NotFound => {
            Err(InvocationNotFoundError(invocation_id.to_string()))?
        }
        client::RestartAsNewInvocationResponse::StillRunning => Err(
            RestartAsNewInvocationStillRunningError(invocation_id.to_string()),
        )?,
        client::RestartAsNewInvocationResponse::Unsupported => Err(
            RestartAsNewInvocationUnsupportedError(invocation_id.to_string()),
        )?,
        client::RestartAsNewInvocationResponse::MissingInput => Err(
            RestartAsNewInvocationMissingInputError(invocation_id.to_string()),
        )?,
        client::RestartAsNewInvocationResponse::NotStarted => Err(
            RestartAsNewInvocationNotStartedError(invocation_id.to_string()),
        )?,
    }
}
