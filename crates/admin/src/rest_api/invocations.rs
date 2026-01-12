// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use restate_admin_rest_model::invocations::{
    BATCH_OPERATION_MAX_SIZE, BatchInvocationRequest, BatchOperationResult,
    BatchRestartAsNewRequest, BatchRestartAsNewResult, BatchResumeRequest,
    FailedInvocationOperation, PatchDeploymentId, RestartAsNewInvocationResponse,
    RestartedInvocation,
};
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
use restate_types::invocation::client::{
    self, CancelInvocationResponse, InvocationClient, KillInvocationResponse,
    PauseInvocationResponse, PurgeInvocationResponse, ResumeInvocationResponse,
};
use restate_types::journal_v2::EntryIndex;
use serde::Deserialize;

use super::error::*;
use crate::generate_meta_api_error;
use crate::state::AdminServiceState;
use futures::future;

generate_meta_api_error!(KillInvocationError: [InvocationNotFoundError, InvocationClientError, InvalidFieldError, InvocationWasAlreadyCompletedError]);

/// Kill an invocation
///
/// Forcefully terminates an invocation. **Warning**: This operation does not guarantee consistency for virtual object instance state,
/// in-flight invocations to other services, or other side effects. Use with caution.
/// For more information, see the [cancellation documentation](https://docs.restate.dev/services/invocation/managing-invocations#kill).
#[utoipa::path(
    patch,
    path = "/invocations/{invocation_id}/kill",
    operation_id = "kill_invocation",
    tag = "invocation",
    params(
        ("invocation_id" = String, Path, description = "Invocation identifier."),
    ),
    responses(
        (status = 200, description = "Invocation killed successfully"),
        KillInvocationError
    )
)]
pub async fn kill_invocation<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(invocation_id): Path<String>,
) -> Result<(), KillInvocationError>
where
    Invocations: InvocationClient,
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
///
/// Gracefully cancels an invocation. The invocation is terminated, but its progress is persisted, allowing consistency guarantees to be maintained.
/// For more information, see the [cancellation documentation](https://docs.restate.dev/services/invocation/managing-invocations#cancel).
#[utoipa::path(
    patch,
    path = "/invocations/{invocation_id}/cancel",
    operation_id = "cancel_invocation",
    tag = "invocation",
    params(
        ("invocation_id" = String, Path, description = "Invocation identifier."),
    ),
    responses(
        (status = 200, description = "Invocation cancelled successfully"),
        (status = 202, description = "Cancellation request accepted and will be processed asynchronously"),
        CancelInvocationError
    )
)]
pub async fn cancel_invocation<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(invocation_id): Path<String>,
) -> Result<StatusCode, CancelInvocationError>
where
    Invocations: InvocationClient,
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

/// Purge a completed invocation
///
/// Deletes all state associated with a completed invocation, including its journal and metadata.
/// This operation only applies to invocations that have already completed. For more information,
/// see the [purging documentation](https://docs.restate.dev/services/invocation/managing-invocations#purge).
#[utoipa::path(
    patch,
    path = "/invocations/{invocation_id}/purge",
    operation_id = "purge_invocation",
    tag = "invocation",
    params(
        ("invocation_id" = String, Path, description = "Invocation identifier."),
    ),
    responses(
        (status = 200, description = "Invocation purged successfully"),
        PurgeInvocationError
    )
)]
pub async fn purge_invocation<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(invocation_id): Path<String>,
) -> Result<(), PurgeInvocationError>
where
    Invocations: InvocationClient,
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

/// Purge invocation journal
///
/// Deletes only the journal entries for a completed invocation, while retaining its metadata.
/// This operation only applies to invocations that have already completed.
#[utoipa::path(
    patch,
    path = "/invocations/{invocation_id}/purge-journal",
    operation_id = "purge_journal",
    tag = "invocation",
    params(
        ("invocation_id" = String, Path, description = "Invocation identifier."),
    ),
    responses(
        (status = 200, description = "Invocation journal purged successfully, metadata retained"),
        PurgeJournalError
    )
)]
pub async fn purge_journal<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(invocation_id): Path<String>,
) -> Result<(), PurgeJournalError>
where
    Invocations: InvocationClient,
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

#[derive(Debug, Default, Deserialize, utoipa::IntoParams)]
pub struct RestartAsNewInvocationQueryParams {
    /// From which entry index the invocation should restart from.
    /// By default the invocation restarts from the beginning (equivalent to 'from = 0'), retaining only the input of the original invocation.
    /// When greater than 0, the new invocation will copy the old journal prefix up to 'from' included, plus eventual completions for commands in the given prefix.
    /// If the journal prefix contains commands that have not been completed, this operation will fail.
    pub from: Option<EntryIndex>,
    /// When restarting from journal prefix, provide a deployment id to use to replace the currently pinned deployment id.
    /// If 'latest', use the latest deployment id. If 'keep', keeps the pinned deployment id.
    /// When not provided, the invocation will resume on latest.
    /// Note: this parameter can be used only in combination with 'from'.
    // TODO inline is a workaround for https://github.com/juhaku/utoipa/issues/1388
    #[param(inline)]
    pub deployment: Option<PatchDeploymentId>,
}

generate_meta_api_error!(RestartInvocationError: [
    InvocationNotFoundError,
    InvocationClientError,
    InvalidFieldError,
    InvalidQueryParameterError,
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

/// Restart invocation as new
///
/// Creates a new invocation from a completed invocation, optionally copying partial progress from the original invocation's journal.
/// The new invocation will have a different invocation ID. Use the `from` parameter to specify how much of the original journal to preserve.
#[utoipa::path(
    patch,
    path = "/invocations/{invocation_id}/restart-as-new",
    operation_id = "restart_as_new_invocation",
    tag = "invocation",
    params(
        ("invocation_id" = String, Path, description = "Invocation identifier."),
        RestartAsNewInvocationQueryParams
    ),
    responses(
        (status = 200, description = "Invocation restarted successfully with a new invocation ID", body = RestartAsNewInvocationResponse),
        RestartInvocationError
    )
)]
pub async fn restart_as_new_invocation<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(invocation_id): Path<String>,
    Query(RestartAsNewInvocationQueryParams { from, deployment }): Query<
        RestartAsNewInvocationQueryParams,
    >,
) -> Result<Json<RestartAsNewInvocationResponse>, RestartInvocationError>
where
    Invocations: InvocationClient,
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
                .into_client()
                .map_err(|err| InvalidQueryParameterError("deployment", err))?,
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

#[derive(Debug, Default, Deserialize, utoipa::IntoParams)]
pub struct ResumeInvocationQueryParams {
    /// When resuming from paused/suspended, provide a deployment id to use to replace the currently pinned deployment id.
    /// If 'latest', use the latest deployment id. If 'keep', keeps the pinned deployment id.
    /// When not provided, the invocation will resume on the pinned deployment id.
    /// When provided and the invocation is either running, or no deployment is pinned, this operation will fail.
    // TODO inline is a workaround for https://github.com/juhaku/utoipa/issues/1388
    #[param(inline)]
    pub deployment: Option<PatchDeploymentId>,
}

generate_meta_api_error!(ResumeInvocationError: [
    InvocationNotFoundError,
    InvocationClientError,
    InvalidFieldError,
    InvalidQueryParameterError,
    ResumeInvocationNotStartedError,
    ResumeInvocationCompletedError,
    ResumeInvocationCannotChangeDeploymentIdError,
    ResumeInvocationDeploymentNotFoundError,
    ResumeInvocationIncompatibleDeploymentIdError
]);

/// Resume an invocation
///
/// Resumes a paused or suspended invocation. If the invocation is backing off due to a retry, this will immediately trigger the retry.
/// Optionally, you can change the deployment ID that will be used when the invocation resumes. For more information see [resume documentation](https://docs.restate.dev/services/invocation/managing-invocations#resume)
#[utoipa::path(
    patch,
    path = "/invocations/{invocation_id}/resume",
    operation_id = "resume_invocation",
    tag = "invocation",
    params(
        ("invocation_id" = String, Path, description = "Invocation identifier."),
        ResumeInvocationQueryParams
    ),
    responses(
        (status = 200, description = "Invocation resumed successfully"),
        ResumeInvocationError
    )
)]
pub async fn resume_invocation<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(invocation_id): Path<String>,
    Query(ResumeInvocationQueryParams { deployment }): Query<ResumeInvocationQueryParams>,
) -> Result<(), ResumeInvocationError>
where
    Invocations: InvocationClient,
{
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| InvalidFieldError("invocation_id", e.to_string()))?;

    match state
        .invocation_client
        .resume_invocation(
            PartitionProcessorRpcRequestId::new(),
            invocation_id,
            deployment
                .unwrap_or_default()
                .into_client()
                .map_err(|err| InvalidQueryParameterError("deployment", err))?,
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

generate_meta_api_error!(PauseInvocationError: [
    InvocationNotFoundError,
    InvocationClientError,
    InvalidFieldError,
    PauseInvocationNotRunningError,
]);

/// Pause an invocation
#[utoipa::path(
    patch,
    path = "/invocations/{invocation_id}/pause",
    operation_id = "pause_invocation",
    tag = "invocation",
    params(
        ("invocation_id" = String, Path, description = "Invocation identifier."),
    ),
    responses(
        (status = 200, description = "Invocation is already paused"),
        (status = 202, description = "Pausing invocation"),
        PauseInvocationError,
    )
)]
pub async fn pause_invocation<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(invocation_id): Path<String>,
) -> Result<StatusCode, PauseInvocationError>
where
    Invocations: InvocationClient,
{
    let invocation_id = invocation_id
        .parse::<InvocationId>()
        .map_err(|e| InvalidFieldError("invocation_id", e.to_string()))?;

    match state
        .invocation_client
        .pause_invocation(PartitionProcessorRpcRequestId::new(), invocation_id)
        .await
        .map_err(InvocationClientError)?
    {
        PauseInvocationResponse::Accepted => {}
        PauseInvocationResponse::NotFound => {
            Err(InvocationNotFoundError(invocation_id.to_string()))?
        }
        PauseInvocationResponse::NotRunning => {
            Err(PauseInvocationNotRunningError(invocation_id.to_string()))?
        }
        PauseInvocationResponse::AlreadyPaused => return Ok(StatusCode::OK),
    };

    Ok(StatusCode::ACCEPTED)
}

// --- Batch operation handlers (internal, not documented in OpenAPI) ---

generate_meta_api_error!(BatchKillInvocationsError: [BatchTooLargeError, InvocationClientError]);

/// Kill multiple invocations in batch (internal endpoint, not in OpenAPI)
pub async fn batch_kill_invocations<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(request): Json<BatchInvocationRequest>,
) -> Result<Json<BatchOperationResult>, BatchKillInvocationsError>
where
    Invocations: InvocationClient + Clone,
{
    if request.invocation_ids.len() > BATCH_OPERATION_MAX_SIZE {
        return Err(
            BatchTooLargeError(request.invocation_ids.len(), BATCH_OPERATION_MAX_SIZE).into(),
        );
    }

    let futures = request.invocation_ids.into_iter().map(|invocation_id| {
        let client = state.invocation_client.clone();
        async move {
            let result = client
                .kill_invocation(PartitionProcessorRpcRequestId::new(), invocation_id)
                .await;
            (invocation_id, result)
        }
    });

    let results = future::join_all(futures).await;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (invocation_id, result) in results {
        match result {
            Ok(KillInvocationResponse::Ok) => succeeded.push(invocation_id),
            Ok(KillInvocationResponse::NotFound) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' not found", invocation_id),
                });
            }
            Ok(KillInvocationResponse::AlreadyCompleted) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' was already completed", invocation_id),
                });
            }
            Err(e) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(Json(BatchOperationResult { succeeded, failed }))
}

generate_meta_api_error!(BatchCancelInvocationsError: [BatchTooLargeError, InvocationClientError]);

/// Cancel multiple invocations in batch (internal endpoint, not in OpenAPI)
pub async fn batch_cancel_invocations<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(request): Json<BatchInvocationRequest>,
) -> Result<Json<BatchOperationResult>, BatchCancelInvocationsError>
where
    Invocations: InvocationClient + Clone,
{
    if request.invocation_ids.len() > BATCH_OPERATION_MAX_SIZE {
        return Err(
            BatchTooLargeError(request.invocation_ids.len(), BATCH_OPERATION_MAX_SIZE).into(),
        );
    }

    let futures = request.invocation_ids.into_iter().map(|invocation_id| {
        let client = state.invocation_client.clone();
        async move {
            let result = client
                .cancel_invocation(PartitionProcessorRpcRequestId::new(), invocation_id)
                .await;
            (invocation_id, result)
        }
    });

    let results = future::join_all(futures).await;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (invocation_id, result) in results {
        match result {
            Ok(CancelInvocationResponse::Done | CancelInvocationResponse::Appended) => {
                succeeded.push(invocation_id)
            }
            Ok(CancelInvocationResponse::NotFound) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' not found", invocation_id),
                });
            }
            Ok(CancelInvocationResponse::AlreadyCompleted) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' was already completed", invocation_id),
                });
            }
            Err(e) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(Json(BatchOperationResult { succeeded, failed }))
}

generate_meta_api_error!(BatchPurgeInvocationsError: [BatchTooLargeError, InvocationClientError]);

/// Purge multiple invocations in batch (internal endpoint, not in OpenAPI)
pub async fn batch_purge_invocations<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(request): Json<BatchInvocationRequest>,
) -> Result<Json<BatchOperationResult>, BatchPurgeInvocationsError>
where
    Invocations: InvocationClient + Clone,
{
    if request.invocation_ids.len() > BATCH_OPERATION_MAX_SIZE {
        return Err(
            BatchTooLargeError(request.invocation_ids.len(), BATCH_OPERATION_MAX_SIZE).into(),
        );
    }

    let futures = request.invocation_ids.into_iter().map(|invocation_id| {
        let client = state.invocation_client.clone();
        async move {
            let result = client
                .purge_invocation(PartitionProcessorRpcRequestId::new(), invocation_id)
                .await;
            (invocation_id, result)
        }
    });

    let results = future::join_all(futures).await;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (invocation_id, result) in results {
        match result {
            Ok(PurgeInvocationResponse::Ok) => succeeded.push(invocation_id),
            Ok(PurgeInvocationResponse::NotFound) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' not found", invocation_id),
                });
            }
            Ok(PurgeInvocationResponse::NotCompleted) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' is not yet completed", invocation_id),
                });
            }
            Err(e) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(Json(BatchOperationResult { succeeded, failed }))
}

generate_meta_api_error!(BatchPurgeJournalError: [BatchTooLargeError, InvocationClientError]);

/// Purge journals for multiple invocations in batch (internal endpoint, not in OpenAPI)
pub async fn batch_purge_journal<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(request): Json<BatchInvocationRequest>,
) -> Result<Json<BatchOperationResult>, BatchPurgeJournalError>
where
    Invocations: InvocationClient + Clone,
{
    if request.invocation_ids.len() > BATCH_OPERATION_MAX_SIZE {
        return Err(
            BatchTooLargeError(request.invocation_ids.len(), BATCH_OPERATION_MAX_SIZE).into(),
        );
    }

    let futures = request.invocation_ids.into_iter().map(|invocation_id| {
        let client = state.invocation_client.clone();
        async move {
            let result = client
                .purge_journal(PartitionProcessorRpcRequestId::new(), invocation_id)
                .await;
            (invocation_id, result)
        }
    });

    let results = future::join_all(futures).await;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (invocation_id, result) in results {
        match result {
            Ok(PurgeInvocationResponse::Ok) => succeeded.push(invocation_id),
            Ok(PurgeInvocationResponse::NotFound) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' not found", invocation_id),
                });
            }
            Ok(PurgeInvocationResponse::NotCompleted) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' is not yet completed", invocation_id),
                });
            }
            Err(e) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(Json(BatchOperationResult { succeeded, failed }))
}

generate_meta_api_error!(BatchRestartAsNewError: [BatchTooLargeError, InvocationClientError, InvalidFieldError]);

/// Restart multiple invocations as new in batch (internal endpoint, not in OpenAPI)
pub async fn batch_restart_as_new_invocations<
    Metadata,
    Discovery,
    Telemetry,
    Invocations,
    Transport,
>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(request): Json<BatchRestartAsNewRequest>,
) -> Result<Json<BatchRestartAsNewResult>, BatchRestartAsNewError>
where
    Invocations: InvocationClient + Clone,
{
    if request.invocation_ids.len() > BATCH_OPERATION_MAX_SIZE {
        return Err(
            BatchTooLargeError(request.invocation_ids.len(), BATCH_OPERATION_MAX_SIZE).into(),
        );
    }

    // Parse the deployment option once (applies to all invocations)
    let deployment = request
        .deployment
        .unwrap_or(PatchDeploymentId::Latest)
        .into_client()
        .map_err(|e| InvalidFieldError("deployment", e))?;

    let futures = request.invocation_ids.into_iter().map(|invocation_id| {
        let client = state.invocation_client.clone();
        let deployment = deployment.clone();
        async move {
            let result = client
                .restart_as_new_invocation(
                    PartitionProcessorRpcRequestId::new(),
                    invocation_id,
                    EntryIndex::default(), // Always restart from the beginning
                    deployment,
                )
                .await;
            (invocation_id, result)
        }
    });

    let results = future::join_all(futures).await;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (invocation_id, result) in results {
        match result {
            Ok(client::RestartAsNewInvocationResponse::Ok { new_invocation_id }) => {
                succeeded.push(RestartedInvocation {
                    old_invocation_id: invocation_id,
                    new_invocation_id,
                });
            }
            Ok(client::RestartAsNewInvocationResponse::NotFound) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' not found", invocation_id),
                });
            }
            Ok(client::RestartAsNewInvocationResponse::StillRunning) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' is still running", invocation_id),
                });
            }
            Ok(client::RestartAsNewInvocationResponse::Unsupported) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Restarting invocation '{}' is not supported", invocation_id),
                });
            }
            Ok(client::RestartAsNewInvocationResponse::MissingInput) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Invocation '{}' cannot be restarted because the input is not available",
                        invocation_id
                    ),
                });
            }
            Ok(client::RestartAsNewInvocationResponse::NotStarted) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Invocation '{}' cannot be restarted because it's not running yet",
                        invocation_id
                    ),
                });
            }
            Ok(client::RestartAsNewInvocationResponse::JournalIndexOutOfRange) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Journal index out of range for invocation '{}'",
                        invocation_id
                    ),
                });
            }
            Ok(client::RestartAsNewInvocationResponse::JournalCopyRangeInvalid) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Journal copy range invalid for invocation '{}'",
                        invocation_id
                    ),
                });
            }
            Ok(client::RestartAsNewInvocationResponse::CannotPatchDeploymentId) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Cannot change deployment ID for invocation '{}'",
                        invocation_id
                    ),
                });
            }
            Ok(client::RestartAsNewInvocationResponse::DeploymentNotFound) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Deployment not found when restarting invocation '{}'",
                        invocation_id
                    ),
                });
            }
            Ok(client::RestartAsNewInvocationResponse::IncompatibleDeploymentId {
                pinned_protocol_version,
                deployment_id,
                supported_protocol_versions,
            }) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Invocation '{}' is running on protocol version '{}', while the chosen deployment '{}' supports the range {:?}",
                        invocation_id, pinned_protocol_version, deployment_id, supported_protocol_versions
                    ),
                });
            }
            Err(e) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(Json(BatchRestartAsNewResult { succeeded, failed }))
}

generate_meta_api_error!(BatchResumeInvocationsError: [BatchTooLargeError, InvocationClientError, InvalidFieldError]);

/// Resume multiple invocations in batch (internal endpoint, not in OpenAPI)
pub async fn batch_resume_invocations<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(request): Json<BatchResumeRequest>,
) -> Result<Json<BatchOperationResult>, BatchResumeInvocationsError>
where
    Invocations: InvocationClient + Clone,
{
    if request.invocation_ids.len() > BATCH_OPERATION_MAX_SIZE {
        return Err(
            BatchTooLargeError(request.invocation_ids.len(), BATCH_OPERATION_MAX_SIZE).into(),
        );
    }

    // Parse the deployment option once (applies to all invocations)
    let deployment = request
        .deployment
        .unwrap_or_default()
        .into_client()
        .map_err(|e| InvalidFieldError("deployment", e))?;

    let futures = request.invocation_ids.into_iter().map(|invocation_id| {
        let client = state.invocation_client.clone();
        let deployment = deployment.clone();
        async move {
            let result = client
                .resume_invocation(
                    PartitionProcessorRpcRequestId::new(),
                    invocation_id,
                    deployment,
                )
                .await;
            (invocation_id, result)
        }
    });

    let results = future::join_all(futures).await;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (invocation_id, result) in results {
        match result {
            Ok(ResumeInvocationResponse::Ok) => succeeded.push(invocation_id),
            Ok(ResumeInvocationResponse::NotFound) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' not found", invocation_id),
                });
            }
            Ok(ResumeInvocationResponse::NotStarted) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Invocation '{}' is either inboxed or scheduled, cannot be resumed",
                        invocation_id
                    ),
                });
            }
            Ok(ResumeInvocationResponse::Completed) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Invocation '{}' is completed, cannot be resumed",
                        invocation_id
                    ),
                });
            }
            Ok(ResumeInvocationResponse::CannotChangeDeploymentId) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Cannot change deployment ID for invocation '{}'",
                        invocation_id
                    ),
                });
            }
            Ok(ResumeInvocationResponse::DeploymentNotFound) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Deployment not found when resuming invocation '{}'",
                        invocation_id
                    ),
                });
            }
            Ok(ResumeInvocationResponse::IncompatibleDeploymentId {
                pinned_protocol_version,
                deployment_id,
                supported_protocol_versions,
            }) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Invocation '{}' is running on protocol version '{}', while the chosen deployment '{}' supports the range {:?}",
                        invocation_id, pinned_protocol_version, deployment_id, supported_protocol_versions
                    ),
                });
            }
            Err(e) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(Json(BatchOperationResult { succeeded, failed }))
}

generate_meta_api_error!(BatchPauseInvocationsError: [BatchTooLargeError, InvocationClientError]);

/// Pause multiple invocations in batch (internal endpoint, not in OpenAPI)
pub async fn batch_pause_invocations<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(request): Json<BatchInvocationRequest>,
) -> Result<Json<BatchOperationResult>, BatchPauseInvocationsError>
where
    Invocations: InvocationClient + Clone,
{
    if request.invocation_ids.len() > BATCH_OPERATION_MAX_SIZE {
        return Err(
            BatchTooLargeError(request.invocation_ids.len(), BATCH_OPERATION_MAX_SIZE).into(),
        );
    }

    let futures = request.invocation_ids.into_iter().map(|invocation_id| {
        let client = state.invocation_client.clone();
        async move {
            let result = client
                .pause_invocation(PartitionProcessorRpcRequestId::new(), invocation_id)
                .await;
            (invocation_id, result)
        }
    });

    let results = future::join_all(futures).await;

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for (invocation_id, result) in results {
        match result {
            Ok(PauseInvocationResponse::Accepted | PauseInvocationResponse::AlreadyPaused) => {
                succeeded.push(invocation_id)
            }
            Ok(PauseInvocationResponse::NotFound) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!("Invocation '{}' not found", invocation_id),
                });
            }
            Ok(PauseInvocationResponse::NotRunning) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: format!(
                        "Invocation '{}' is not running, cannot be paused",
                        invocation_id
                    ),
                });
            }
            Err(e) => {
                failed.push(FailedInvocationOperation {
                    invocation_id,
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(Json(BatchOperationResult { succeeded, failed }))
}
