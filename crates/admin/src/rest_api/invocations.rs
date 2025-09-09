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
use restate_types::identifiers::{
    DeploymentId, InvocationId, PartitionProcessorRpcRequestId, WithPartitionKey,
};
use restate_types::invocation::client::{
    self, CancelInvocationResponse, InvocationClient, KillInvocationResponse,
    PurgeInvocationResponse, ResumeInvocationResponse,
};
use restate_types::invocation::{InvocationTermination, PurgeInvocationRequest, TerminationFlavor};
use restate_types::journal_v2::EntryIndex;
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

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub enum PatchDeploymentId {
    #[default]
    #[serde(alias = "keep")]
    Keep,
    #[serde(alias = "latest")]
    Latest,
    #[serde(untagged)]
    Id(String),
}

impl PatchDeploymentId {
    pub fn into_client(self) -> Result<client::PatchDeploymentId, InvalidFieldError> {
        Ok(match self {
            PatchDeploymentId::Keep => client::PatchDeploymentId::KeepPinned,
            PatchDeploymentId::Latest => client::PatchDeploymentId::PinToLatest,
            PatchDeploymentId::Id(dp_id) => client::PatchDeploymentId::PinTo {
                id: dp_id
                    .parse::<DeploymentId>()
                    .map_err(|e| InvalidFieldError("deployment_id", e.to_string()))?,
            },
        })
    }
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct RestartAsNewInvocationQueryParams {
    pub from: Option<EntryIndex>,
    pub deployment: Option<PatchDeploymentId>,
}

generate_meta_api_error!(RestartInvocationError: [
    InvocationNotFoundError,
    InvocationClientError,
    InvalidFieldError,
    RestartAsNewInvocationStillRunningError,
    RestartAsNewInvocationUnsupportedError,
    RestartAsNewInvocationMissingInputError,
    RestartAsNewInvocationNotStartedError,
    RestartAsNewInvocationJournalIndexOutOfRangeError,
    RestartAsNewInvocationJournalCopyRangeInvalidError,
    RestartAsNewInvocationCannotChangeDeploymentIdError,
    RestartAsNewInvocationDeploymentNotFoundError,
    RestartAsNewInvocationIncompatibleDeploymentIdError
]);

/// Restart an invocation
#[openapi(
    summary = "Restart as new invocation",
    description = "Restart the given invocation as new. \
    This will restart the invocation as a new invocation with a different invocation id. \
    By using the 'from' query parameter, some of the partial progress can be copied over to the new invocation.",
    operation_id = "restart_as_new_invocation",
    tags = "invocation",
    parameters(
        path(
            name = "invocation_id",
            description = "Invocation identifier.",
            schema = "std::string::String"
        ),
        query(
            name = "from",
            description = "From which entry index the invocation should restart from. \
            By default the invocation restarts from the beginning (equivalent to 'from = 0'), retaining only the input of the original invocation. \
            When greater than 0, the new invocation will copy the old journal prefix up to 'from' included, plus eventual completions for commands in the given prefix. \
            If the journal prefix contains commands that have not been completed, this operation will fail.",
            required = false,
            style = "simple",
            allow_empty_value = false,
            schema = "u32",
        ),
        query(
            name = "deployment",
            description = "When restarting from journal prefix, provide a deployment id to use to replace the currently pinned deployment id. \
            If 'latest', use the latest deployment id. If 'keep', keeps the pinned deployment id. \
            When not provided, the invocation will resume on latest. \
            Note: this parameter can be used only in combination with 'from'.",
            required = false,
            style = "simple",
            allow_empty_value = false,
            schema = "PatchDeploymentId",
        ),
    )
)]
pub async fn restart_as_new_invocation<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(invocation_id): Path<String>,
    Query(RestartAsNewInvocationQueryParams { from, deployment }): Query<
        RestartAsNewInvocationQueryParams,
    >,
) -> Result<Json<RestartAsNewInvocationResponse>, RestartInvocationError>
where
    IC: InvocationClient,
{
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| InvalidFieldError("invocation_id", e.to_string()))?;

    match state
        .invocation_client
        .restart_as_new_invocation(
            PartitionProcessorRpcRequestId::new(),
            invocation_id,
            from.unwrap_or_default(),
            deployment
                .unwrap_or(PatchDeploymentId::Latest)
                .into_client()?,
        )
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
        client::RestartAsNewInvocationResponse::JournalIndexOutOfRange => Err(
            RestartAsNewInvocationJournalIndexOutOfRangeError(invocation_id.to_string()),
        )?,
        client::RestartAsNewInvocationResponse::JournalCopyRangeInvalid => Err(
            RestartAsNewInvocationJournalCopyRangeInvalidError(invocation_id.to_string()),
        )?,
        client::RestartAsNewInvocationResponse::CannotPatchDeploymentId => Err(
            RestartAsNewInvocationCannotChangeDeploymentIdError(invocation_id.to_string()),
        )?,
        client::RestartAsNewInvocationResponse::DeploymentNotFound => Err(
            RestartAsNewInvocationDeploymentNotFoundError(invocation_id.to_string()),
        )?,
        client::RestartAsNewInvocationResponse::IncompatibleDeploymentId {
            pinned_protocol_version,
            deployment_id,
            supported_protocol_versions,
        } => Err(RestartAsNewInvocationIncompatibleDeploymentIdError {
            invocation_id: invocation_id.to_string(),
            pinned_protocol_version,
            deployment_id: deployment_id.to_string(),
            supported_protocol_versions,
        })?,
    }
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct ResumeInvocationQueryParams {
    pub deployment: Option<PatchDeploymentId>,
}

generate_meta_api_error!(ResumeInvocationError: [
    InvocationNotFoundError,
    InvocationClientError,
    InvalidFieldError,
    ResumeInvocationNotStartedError,
    ResumeInvocationCompletedError,
    ResumeInvocationCannotChangeDeploymentIdError,
    ResumeInvocationDeploymentNotFoundError,
    ResumeInvocationIncompatibleDeploymentIdError
]);

/// Resume an invocation
#[openapi(
    summary = "Resume an invocation",
    description = "Resume the given invocation. In case the invocation is backing-off, this will immediately trigger the retry timer. If the invocation is suspended or paused, this will resume it.",
    operation_id = "resume_invocation",
    tags = "invocation",
    parameters(
        path(
            name = "invocation_id",
            description = "Invocation identifier.",
            schema = "std::string::String"
        ),
        query(
            name = "deployment",
            description = "When resuming from paused/suspended, provide a deployment id to use to replace the currently pinned deployment id. \
            If 'latest', use the latest deployment id. If 'keep', keeps the pinned deployment id. \
            When not provided, the invocation will resume on the pinned deployment id.\
            When provided and the invocation is either running, or no deployment is pinned, this operation will fail.",
            required = false,
            style = "simple",
            allow_empty_value = false,
            schema = "PatchDeploymentId",
        )
    )
)]
pub async fn resume_invocation<V, IC>(
    State(state): State<AdminServiceState<V, IC>>,
    Path(invocation_id): Path<String>,
    Query(ResumeInvocationQueryParams { deployment }): Query<ResumeInvocationQueryParams>,
) -> Result<(), ResumeInvocationError>
where
    IC: InvocationClient,
{
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| InvalidFieldError("invocation_id", e.to_string()))?;

    match state
        .invocation_client
        .resume_invocation(
            PartitionProcessorRpcRequestId::new(),
            invocation_id,
            deployment.unwrap_or_default().into_client()?,
        )
        .await
        .map_err(InvocationClientError)?
    {
        ResumeInvocationResponse::Ok => {}
        ResumeInvocationResponse::NotFound => {
            Err(InvocationNotFoundError(invocation_id.to_string()))?
        }
        ResumeInvocationResponse::NotStarted => {
            Err(ResumeInvocationNotStartedError(invocation_id.to_string()))?
        }
        ResumeInvocationResponse::Completed => {
            Err(ResumeInvocationCompletedError(invocation_id.to_string()))?
        }
        ResumeInvocationResponse::CannotChangeDeploymentId => Err(
            ResumeInvocationCannotChangeDeploymentIdError(invocation_id.to_string()),
        )?,
        ResumeInvocationResponse::DeploymentNotFound => Err(
            ResumeInvocationDeploymentNotFoundError(invocation_id.to_string()),
        )?,
        ResumeInvocationResponse::IncompatibleDeploymentId {
            pinned_protocol_version,
            deployment_id,
            supported_protocol_versions,
        } => Err(ResumeInvocationIncompatibleDeploymentIdError {
            invocation_id: invocation_id.to_string(),
            pinned_protocol_version,
            deployment_id: deployment_id.to_string(),
            supported_protocol_versions,
        })?,
    };

    Ok(())
}
