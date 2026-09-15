// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The typed partition processor requests. Each of them knows how to lower itself onto the wire
//! and how to lift the wire response into its typed response.
//!
//! All requests in this module lower onto the legacy [`PartitionProcessorRpcRequest`] envelope.

use std::sync::Arc;

use crate::identifiers::{
    InvocationId, PartitionId, PartitionKey, PartitionProcessorRpcRequestId, WithPartitionKey,
};
use crate::invocation::client::{
    AttachInvocationResponse, CancelInvocationResponse, GetInvocationOutputResponse,
    GetInvocationStatusResponse, InvocationOutput, KillInvocationResponse, PatchDeploymentId,
    PauseInvocationResponse, PurgeInvocationResponse, RestartAsNewInvocationResponse,
    ResumeInvocationResponse, SubmittedInvocationNotification,
};
use crate::invocation::{InvocationQuery, InvocationRequest, InvocationResponse};
use crate::journal::EntryIndex;
use crate::journal_v2::Signal;
use crate::net::partition_processor::{
    AppendInvocationReplyOn, GetInvocationOutputResponseMode, PartitionProcessorRpcError,
    PartitionProcessorRpcRequest, PartitionProcessorRpcRequestInner, PartitionProcessorRpcResponse,
};

use super::client::{PartitionProcessorRpc, WireResponseError};

type LegacyResponse = Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>;

/// Append the invocation to the log, replying once the partition processor emitted the
/// [`SubmittedInvocationNotification`].
#[derive(Debug, Clone)]
pub struct SubmitInvocation {
    pub invocation_request: Arc<InvocationRequest>,
}

impl WithPartitionKey for SubmitInvocation {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_request.partition_key()
    }
}

impl PartitionProcessorRpc for SubmitInvocation {
    type Response = SubmittedInvocationNotification;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::AppendInvocation(
                self.invocation_request,
                AppendInvocationReplyOn::Submitted,
            ),
        )
    }

    fn from_wire(
        request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        let PartitionProcessorRpcResponse::Submitted(submit_notification) = response? else {
            return Err(WireResponseError::UnexpectedResponse);
        };
        debug_assert_eq!(
            request_id, submit_notification.request_id,
            "Conflicting submit notification received"
        );
        Ok(submit_notification)
    }
}

/// Append the invocation to the log and wait for its output.
#[derive(Debug, Clone)]
pub struct CallInvocation {
    pub invocation_request: Arc<InvocationRequest>,
}

impl WithPartitionKey for CallInvocation {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_request.partition_key()
    }
}

impl PartitionProcessorRpc for CallInvocation {
    type Response = InvocationOutput;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::AppendInvocation(
                self.invocation_request,
                AppendInvocationReplyOn::Output,
            ),
        )
    }

    fn from_wire(
        request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        let PartitionProcessorRpcResponse::Output(invocation_output) = response? else {
            return Err(WireResponseError::UnexpectedResponse);
        };
        debug_assert_eq!(
            request_id, invocation_output.request_id,
            "Conflicting invocation output received"
        );
        Ok(invocation_output)
    }
}

/// Attach to an existing invocation and wait for its output.
#[derive(Debug, Clone)]
pub struct AttachInvocation {
    pub invocation_query: InvocationQuery,
}

impl WithPartitionKey for AttachInvocation {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_query.partition_key()
    }
}

impl PartitionProcessorRpc for AttachInvocation {
    type Response = AttachInvocationResponse;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::GetInvocationOutput(
                self.invocation_query,
                GetInvocationOutputResponseMode::BlockWhenNotReady,
            ),
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        Ok(match response? {
            PartitionProcessorRpcResponse::NotFound => AttachInvocationResponse::NotFound,
            PartitionProcessorRpcResponse::NotSupported => AttachInvocationResponse::NotSupported,
            PartitionProcessorRpcResponse::Output(output) => {
                AttachInvocationResponse::Ready(output)
            }
            _ => return Err(WireResponseError::UnexpectedResponse),
        })
    }
}

/// Get an invocation output, when present.
#[derive(Debug, Clone)]
pub struct GetInvocationOutput {
    pub invocation_query: InvocationQuery,
}

impl WithPartitionKey for GetInvocationOutput {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_query.partition_key()
    }
}

impl PartitionProcessorRpc for GetInvocationOutput {
    type Response = GetInvocationOutputResponse;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::GetInvocationOutput(
                self.invocation_query,
                GetInvocationOutputResponseMode::ReplyIfNotReady,
            ),
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        Ok(match response? {
            PartitionProcessorRpcResponse::NotFound => GetInvocationOutputResponse::NotFound,
            PartitionProcessorRpcResponse::NotSupported => {
                GetInvocationOutputResponse::NotSupported
            }
            PartitionProcessorRpcResponse::NotReady => GetInvocationOutputResponse::NotReady,
            PartitionProcessorRpcResponse::Output(output) => {
                GetInvocationOutputResponse::Ready(output)
            }
            _ => return Err(WireResponseError::UnexpectedResponse),
        })
    }
}

/// Get the invocation status, when present.
#[derive(Debug, Clone)]
pub struct GetInvocationStatus {
    pub invocation_id: InvocationId,
}

impl WithPartitionKey for GetInvocationStatus {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id.partition_key()
    }
}

impl PartitionProcessorRpc for GetInvocationStatus {
    type Response = GetInvocationStatusResponse;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::GetInvocationStatus {
                invocation_id: self.invocation_id,
            },
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        Ok(match response? {
            PartitionProcessorRpcResponse::NotFound => GetInvocationStatusResponse::NotFound,
            PartitionProcessorRpcResponse::Status(status) => {
                GetInvocationStatusResponse::Status(status)
            }
            _ => return Err(WireResponseError::UnexpectedResponse),
        })
    }
}

/// **DEPRECATED** Append an [`InvocationResponse`] to an existing invocation journal.
/// Only ServiceProtocol <= 3.
#[derive(Debug, Clone)]
pub struct AppendInvocationResponse {
    pub invocation_response: InvocationResponse,
}

impl WithPartitionKey for AppendInvocationResponse {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_response.partition_key()
    }
}

impl PartitionProcessorRpc for AppendInvocationResponse {
    type Response = ();
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::AppendInvocationResponse(self.invocation_response),
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        matches!(response?, PartitionProcessorRpcResponse::Appended)
            .then_some(())
            .ok_or(WireResponseError::UnexpectedResponse)
    }
}

/// Append a signal to an existing invocation journal.
#[derive(Debug, Clone)]
pub struct AppendSignal {
    pub invocation_id: InvocationId,
    pub signal: Signal,
}

impl WithPartitionKey for AppendSignal {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id.partition_key()
    }
}

impl PartitionProcessorRpc for AppendSignal {
    type Response = ();
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::AppendSignal(self.invocation_id, self.signal),
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        matches!(response?, PartitionProcessorRpcResponse::Appended)
            .then_some(())
            .ok_or(WireResponseError::UnexpectedResponse)
    }
}

/// Cancel the given invocation.
#[derive(Debug, Clone)]
pub struct CancelInvocation {
    pub invocation_id: InvocationId,
}

impl WithPartitionKey for CancelInvocation {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id.partition_key()
    }
}

impl PartitionProcessorRpc for CancelInvocation {
    type Response = CancelInvocationResponse;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::CancelInvocation {
                invocation_id: self.invocation_id,
            },
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        match response? {
            PartitionProcessorRpcResponse::CancelInvocation(res) => Ok(res.into()),
            _ => Err(WireResponseError::UnexpectedResponse),
        }
    }
}

/// Kill the given invocation.
#[derive(Debug, Clone)]
pub struct KillInvocation {
    pub invocation_id: InvocationId,
}

impl WithPartitionKey for KillInvocation {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id.partition_key()
    }
}

impl PartitionProcessorRpc for KillInvocation {
    type Response = KillInvocationResponse;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::KillInvocation {
                invocation_id: self.invocation_id,
            },
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        match response? {
            PartitionProcessorRpcResponse::KillInvocation(res) => Ok(res.into()),
            _ => Err(WireResponseError::UnexpectedResponse),
        }
    }
}

/// Purge the given invocation. This cleanups all the state for the given invocation.
/// This command applies only to completed invocations.
#[derive(Debug, Clone)]
pub struct PurgeInvocation {
    pub invocation_id: InvocationId,
}

impl WithPartitionKey for PurgeInvocation {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id.partition_key()
    }
}

impl PartitionProcessorRpc for PurgeInvocation {
    type Response = PurgeInvocationResponse;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::PurgeInvocation {
                invocation_id: self.invocation_id,
            },
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        match response? {
            PartitionProcessorRpcResponse::PurgeInvocation(res) => Ok(res.into()),
            _ => Err(WireResponseError::UnexpectedResponse),
        }
    }
}

/// Purge the given invocation journal, retaining the metadata.
/// This command applies only to completed invocations.
#[derive(Debug, Clone)]
pub struct PurgeJournal {
    pub invocation_id: InvocationId,
}

impl WithPartitionKey for PurgeJournal {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id.partition_key()
    }
}

impl PartitionProcessorRpc for PurgeJournal {
    type Response = PurgeInvocationResponse;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::PurgeJournal {
                invocation_id: self.invocation_id,
            },
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        match response? {
            PartitionProcessorRpcResponse::PurgeJournal(res) => Ok(res.into()),
            _ => Err(WireResponseError::UnexpectedResponse),
        }
    }
}

/// Restart the given invocation as a new invocation, with a new invocation id.
#[derive(Debug, Clone)]
pub struct RestartAsNewInvocation {
    pub invocation_id: InvocationId,
    pub copy_prefix_up_to_index_included: EntryIndex,
    pub patch_deployment_id: PatchDeploymentId,
}

impl WithPartitionKey for RestartAsNewInvocation {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id.partition_key()
    }
}

impl PartitionProcessorRpc for RestartAsNewInvocation {
    type Response = RestartAsNewInvocationResponse;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::RestartAsNewInvocation {
                invocation_id: self.invocation_id,
                copy_prefix_up_to_index_included: self.copy_prefix_up_to_index_included,
                patch_deployment_id: self.patch_deployment_id,
            },
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        match response? {
            PartitionProcessorRpcResponse::RestartAsNewInvocation(res) => Ok(res.into()),
            _ => Err(WireResponseError::UnexpectedResponse),
        }
    }
}

/// Resume the given invocation.
#[derive(Debug, Clone)]
pub struct ResumeInvocation {
    pub invocation_id: InvocationId,
    pub deployment_id: PatchDeploymentId,
}

impl WithPartitionKey for ResumeInvocation {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id.partition_key()
    }
}

impl PartitionProcessorRpc for ResumeInvocation {
    type Response = ResumeInvocationResponse;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::ResumeInvocation {
                invocation_id: self.invocation_id,
                deployment_id: self.deployment_id,
            },
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        match response? {
            PartitionProcessorRpcResponse::ResumeInvocation(res) => Ok(res.into()),
            _ => Err(WireResponseError::UnexpectedResponse),
        }
    }
}

/// Pause the given invocation.
#[derive(Debug, Clone)]
pub struct PauseInvocation {
    pub invocation_id: InvocationId,
}

impl WithPartitionKey for PauseInvocation {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id.partition_key()
    }
}

impl PartitionProcessorRpc for PauseInvocation {
    type Response = PauseInvocationResponse;
    type Wire = PartitionProcessorRpcRequest;

    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire {
        PartitionProcessorRpcRequest::new(
            request_id,
            partition_id,
            PartitionProcessorRpcRequestInner::PauseInvocation {
                invocation_id: self.invocation_id,
            },
        )
    }

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        match response? {
            PartitionProcessorRpcResponse::PauseInvocation(res) => Ok(res.into()),
            _ => Err(WireResponseError::UnexpectedResponse),
        }
    }
}
