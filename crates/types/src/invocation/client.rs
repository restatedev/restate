// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::errors::InvocationError;
use crate::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
use crate::invocation::{InvocationQuery, InvocationRequest, InvocationResponse, InvocationTarget};
use crate::journal_v2::Signal;
use crate::time::MillisSinceEpoch;
use bytes::Bytes;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
#[error("{inner}")]
pub struct InvocationClientError {
    is_safe_to_retry: bool,
    #[source]
    inner: anyhow::Error,
}

impl InvocationClientError {
    pub fn new(inner: impl Into<anyhow::Error>, is_safe_to_retry: bool) -> Self {
        Self {
            is_safe_to_retry,
            inner: inner.into(),
        }
    }

    pub fn is_safe_to_retry(&self) -> bool {
        self.is_safe_to_retry
    }

    pub fn into_inner(self) -> anyhow::Error {
        self.inner
    }
}

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
}

/// This trait provides the functionalities to interact with Restate invocations.
pub trait InvocationClient {
    /// Append the invocation to the log, waiting for the PP to emit [`SubmittedInvocationNotification`] when the command is processed.
    fn append_invocation_and_wait_submit_notification(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: Arc<InvocationRequest>,
    ) -> impl Future<Output = Result<SubmittedInvocationNotification, InvocationClientError>> + Send;

    /// Append the invocation and wait for its output.
    fn append_invocation_and_wait_output(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: Arc<InvocationRequest>,
    ) -> impl Future<Output = Result<InvocationOutput, InvocationClientError>> + Send;

    /// Attach to an existing invocation and wait for its output.
    fn attach_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> impl Future<Output = Result<AttachInvocationResponse, InvocationClientError>> + Send;

    /// Get an invocation output, when present.
    fn get_invocation_output(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> impl Future<Output = Result<GetInvocationOutputResponse, InvocationClientError>> + Send;

    /// **DEPRECATED** Append [`InvocationResponse`] to an existing invocation journal. Only ServiceProtocol <= 3
    fn append_invocation_response(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_response: InvocationResponse,
    ) -> impl Future<Output = Result<(), InvocationClientError>> + Send;

    /// Append a signal to an existing invocation journal.
    fn append_signal(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
        signal: Signal,
    ) -> impl Future<Output = Result<(), InvocationClientError>> + Send;

    /// Cancel the given invocation.
    fn cancel_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<CancelInvocationResponse, InvocationClientError>> + Send;

    /// Kill the given invocation.
    fn kill_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<KillInvocationResponse, InvocationClientError>> + Send;

    /// Purge the given invocation. This cleanups all the state for the given invocation. This command applies only to completed invocations.
    fn purge_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<PurgeInvocationResponse, InvocationClientError>> + Send;

    /// Purge the given invocation journal. This cleanups only the journal for the given invocation, retaining the metadata. This command applies only to completed invocations.
    fn purge_journal(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<PurgeInvocationResponse, InvocationClientError>> + Send;

    /// Restart the given invocation as a new invocation, with a new invocation id.
    fn restart_as_new_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<RestartAsNewInvocationResponse, InvocationClientError>> + Send;
}
