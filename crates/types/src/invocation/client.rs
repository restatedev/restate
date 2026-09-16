// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::errors::InvocationError;
use crate::identifiers::{DeploymentId, InvocationId, PartitionProcessorRpcRequestId};
use crate::invocation::{InvocationQuery, InvocationRequest, InvocationResponse, InvocationTarget};
use crate::journal::EntryIndex;
use crate::journal_v2::Signal;
use crate::partition_processor::client::PartitionProcessorClientError;
use crate::time::MillisSinceEpoch;
use bytes::Bytes;
use std::ops::RangeInclusive;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmittedInvocationNotification {
    pub request_id: PartitionProcessorRpcRequestId,
    pub execution_time: Option<MillisSinceEpoch>,
    /// If true, this request_id created a "fresh invocation",
    /// otherwise the invocation was previously submitted.
    pub is_new_invocation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationOutput {
    pub request_id: PartitionProcessorRpcRequestId,
    pub invocation_id: Option<InvocationId>,
    pub completion_expiry_time: Option<MillisSinceEpoch>,
    pub response: InvocationOutputResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum InvocationOutputResponse {
    Success(InvocationTarget, Bytes),
    Failure(InvocationError),
}

// the most used variant is the largest one, so we are muting clippy intentionally.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum AttachInvocationResponse {
    NotFound,
    /// Returned when the invocation hasn't an idempotency key, nor it's a workflow run.
    NotSupported,
    Ready(InvocationOutput),
}

// the most used variant is the largest one, so we are muting clippy intentionally.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum GetInvocationOutputResponse {
    NotFound,
    /// The invocation was found, but it's still processing and a result is not ready yet.
    NotReady,
    /// Returned when the invocation hasn't an idempotency key, nor it's a workflow run.
    NotSupported,
    Ready(InvocationOutput),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum InvocationState {
    Scheduled,
    Inboxed,
    Invoked,
    Suspended,
    Paused,
    Killed,
    Failed,
    Succeeded,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationStatus {
    pub state: InvocationState,
    pub error: Option<InvocationError>,
}

// the most used variant is the largest one, so we are muting clippy intentionally.
#[derive(Debug, Clone)]
pub enum GetInvocationStatusResponse {
    NotFound,
    Status(InvocationStatus),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelInvocationResponse {
    /// The cancellation was processed immediately (e.g. for inboxed/scheduled invocations)
    Done,
    /// The cancel signal was appended
    Appended,
    NotFound,
    AlreadyCompleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KillInvocationResponse {
    Ok,
    NotFound,
    AlreadyCompleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PurgeInvocationResponse {
    Ok,
    NotFound,
    NotCompleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartAsNewInvocationResponse {
    Ok {
        new_invocation_id: InvocationId,
    },
    NotFound,
    /// The invocation cannot be restarted, because it's still running
    StillRunning,
    /// Restart as New is currently unsupported by workflows
    Unsupported,
    /// The invocation is missing the input, thus it cannot be restarted
    MissingInput,
    /// The initial invocation wasn't started yet (it's enqueued or scheduled)
    NotStarted,
    /// Journal index is out of range
    JournalIndexOutOfRange,
    /// The journal prefix cannot be copied over, because it contains a command without a completion
    JournalCopyRangeInvalid,
    /// Cannot patch the deployment id when restarting from index 0
    CannotPatchDeploymentId,
    /// The given deployment was not found
    DeploymentNotFound,
    /// The given deployment is incompatible with the existing journal
    IncompatibleDeploymentId {
        pinned_protocol_version: i32,
        deployment_id: DeploymentId,
        supported_protocol_versions: RangeInclusive<i32>,
    },
}

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PatchDeploymentId {
    #[default]
    KeepPinned,
    PinToLatest,
    PinTo {
        id: DeploymentId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeInvocationResponse {
    Ok,
    NotFound,
    /// The invocation isn't started yet (it's enqueued or scheduled)
    NotStarted,
    /// The user provided a deployment id to override the pinned id,
    /// but it cannot be changed because there is no pinned deployment yet or the invocation is running.
    CannotChangeDeploymentId,
    /// No deployment found for the given service, or the given deployment id doesn't exist.
    DeploymentNotFound,
    /// The chosen deployment id is incompatible
    IncompatibleDeploymentId {
        pinned_protocol_version: i32,
        deployment_id: DeploymentId,
        supported_protocol_versions: RangeInclusive<i32>,
    },
    /// Invocation is completed
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PauseInvocationResponse {
    AlreadyPaused,
    Accepted,
    NotFound,
    /// The invocation is not running
    NotRunning,
}

/// This trait provides the functionalities to interact with Restate invocations.
pub trait InvocationClient {
    /// Append the invocation to the log, waiting for the PP to emit [`SubmittedInvocationNotification`] when the command is processed.
    fn append_invocation_and_wait_submit_notification(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: Arc<InvocationRequest>,
    ) -> impl Future<Output = Result<SubmittedInvocationNotification, PartitionProcessorClientError>>
    + Send;

    /// Append the invocation and wait for its output.
    fn append_invocation_and_wait_output(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: Arc<InvocationRequest>,
    ) -> impl Future<Output = Result<InvocationOutput, PartitionProcessorClientError>> + Send;

    /// Attach to an existing invocation and wait for its output.
    fn attach_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> impl Future<Output = Result<AttachInvocationResponse, PartitionProcessorClientError>> + Send;

    /// Get an invocation output, when present.
    fn get_invocation_output(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> impl Future<Output = Result<GetInvocationOutputResponse, PartitionProcessorClientError>> + Send;

    /// Get invocation status, when present.
    fn get_invocation_status(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<GetInvocationStatusResponse, PartitionProcessorClientError>> + Send;

    /// **DEPRECATED** Append [`InvocationResponse`] to an existing invocation journal. Only ServiceProtocol <= 3
    fn append_invocation_response(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_response: InvocationResponse,
    ) -> impl Future<Output = Result<(), PartitionProcessorClientError>> + Send;

    /// Append a signal to an existing invocation journal.
    fn append_signal(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
        signal: Signal,
    ) -> impl Future<Output = Result<(), PartitionProcessorClientError>> + Send;

    /// Cancel the given invocation.
    fn cancel_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<CancelInvocationResponse, PartitionProcessorClientError>> + Send;

    /// Kill the given invocation.
    fn kill_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<KillInvocationResponse, PartitionProcessorClientError>> + Send;

    /// Purge the given invocation. This cleanups all the state for the given invocation. This command applies only to completed invocations.
    fn purge_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<PurgeInvocationResponse, PartitionProcessorClientError>> + Send;

    /// Purge the given invocation journal. This cleanups only the journal for the given invocation, retaining the metadata. This command applies only to completed invocations.
    fn purge_journal(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<PurgeInvocationResponse, PartitionProcessorClientError>> + Send;

    /// Restart the given invocation as a new invocation, with a new invocation id.
    fn restart_as_new_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
        copy_prefix_up_to_index_included: EntryIndex,
        patch_deployment_id: PatchDeploymentId,
    ) -> impl Future<Output = Result<RestartAsNewInvocationResponse, PartitionProcessorClientError>> + Send;

    /// Resume the given invocation.
    fn resume_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
        resume_invocation_deployment_id: PatchDeploymentId,
    ) -> impl Future<Output = Result<ResumeInvocationResponse, PartitionProcessorClientError>> + Send;

    /// Pause the given invocation.
    fn pause_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<PauseInvocationResponse, PartitionProcessorClientError>> + Send;
}
