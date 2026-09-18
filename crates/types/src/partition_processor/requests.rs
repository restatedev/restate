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
//! Requests lower onto either the legacy [`PartitionProcessorRpcRequest`] envelope or their
//! dedicated wire message.

use std::sync::Arc;

use crate::identifiers::{
    InvocationId, PartitionKey, PartitionProcessorRpcRequestId, WithPartitionKey,
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
use crate::net::RpcRequest;
use crate::net::partition_processor::{
    AppendInvocationReplyOn, AppendInvocationResponseRpcRequest,
    AppendInvocationResponseRpcResponse, AppendInvocationRpcRequest, AppendSignalRpcRequest,
    AppendSignalRpcResponse, CancelInvocationRpcRequest, GetInvocationOutputResponseMode,
    GetInvocationOutputRpcRequest, GetInvocationStatusRpcRequest, KillInvocationRpcRequest,
    PartitionProcessorRpcError, PartitionProcessorRpcRequestHeader,
    PartitionProcessorRpcRequestInner, PartitionProcessorRpcResponse,
    PartitionProcessorWireEnvelope, PauseInvocationRpcRequest, PurgeInvocationRpcRequest,
    PurgeJournalRpcRequest, RestartAsNewInvocationRpcRequest, ResumeInvocationRpcRequest,
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = SubmittedInvocationNotification;
    type Wire = AppendInvocationRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::AppendInvocation(
            self.invocation_request,
            AppendInvocationReplyOn::Submitted,
        ))
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        AppendInvocationRpcRequest {
            header,
            invocation_request: self.invocation_request,
            append_invocation_reply_on: AppendInvocationReplyOn::Submitted,
        }
    }

    fn from_legacy_response(
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

    fn from_wire(
        request_id: PartitionProcessorRpcRequestId,
        response: <Self::Wire as RpcRequest>::Response,
    ) -> Result<Self::Response, WireResponseError> {
        let submit_notification = Self::Response::try_from(response.into_result()?)?;
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = InvocationOutput;
    type Wire = AppendInvocationRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::AppendInvocation(
            self.invocation_request,
            AppendInvocationReplyOn::Output,
        ))
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        AppendInvocationRpcRequest {
            header,
            invocation_request: self.invocation_request,
            append_invocation_reply_on: AppendInvocationReplyOn::Output,
        }
    }

    fn from_legacy_response(
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

    fn from_wire(
        request_id: PartitionProcessorRpcRequestId,
        response: <Self::Wire as RpcRequest>::Response,
    ) -> Result<Self::Response, WireResponseError> {
        let invocation_output = Self::Response::try_from(response.into_result()?)?;
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = AttachInvocationResponse;
    type Wire = GetInvocationOutputRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::GetInvocationOutput(
            self.invocation_query,
            GetInvocationOutputResponseMode::BlockWhenNotReady,
        ))
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        GetInvocationOutputRpcRequest {
            header,
            invocation_query: self.invocation_query,
            response_mode: GetInvocationOutputResponseMode::BlockWhenNotReady,
        }
    }

    fn from_legacy_response(
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = GetInvocationOutputResponse;
    type Wire = GetInvocationOutputRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::GetInvocationOutput(
            self.invocation_query,
            GetInvocationOutputResponseMode::ReplyIfNotReady,
        ))
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        GetInvocationOutputRpcRequest {
            header,
            invocation_query: self.invocation_query,
            response_mode: GetInvocationOutputResponseMode::ReplyIfNotReady,
        }
    }

    fn from_legacy_response(
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = GetInvocationStatusResponse;
    type Wire = GetInvocationStatusRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::GetInvocationStatus {
            invocation_id: self.invocation_id,
        })
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        GetInvocationStatusRpcRequest {
            header,
            invocation_id: self.invocation_id,
        }
    }

    fn from_legacy_response(
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = AppendInvocationResponseRpcResponse;
    type Wire = AppendInvocationResponseRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::AppendInvocationResponse(
            self.invocation_response,
        ))
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        AppendInvocationResponseRpcRequest {
            header,
            invocation_response: self.invocation_response,
        }
    }

    fn from_legacy_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        matches!(response?, PartitionProcessorRpcResponse::Appended)
            .then_some(AppendInvocationResponseRpcResponse)
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = AppendSignalRpcResponse;
    type Wire = AppendSignalRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::AppendSignal(
            self.invocation_id,
            self.signal,
        ))
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        AppendSignalRpcRequest {
            header,
            invocation_id: self.invocation_id,
            signal: self.signal,
        }
    }

    fn from_legacy_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        matches!(response?, PartitionProcessorRpcResponse::Appended)
            .then_some(AppendSignalRpcResponse)
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = CancelInvocationResponse;
    type Wire = CancelInvocationRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::CancelInvocation {
            invocation_id: self.invocation_id,
        })
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        CancelInvocationRpcRequest {
            header,
            invocation_id: self.invocation_id,
        }
    }

    fn from_legacy_response(
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = KillInvocationResponse;
    type Wire = KillInvocationRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::KillInvocation {
            invocation_id: self.invocation_id,
        })
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        KillInvocationRpcRequest {
            header,
            invocation_id: self.invocation_id,
        }
    }

    fn from_legacy_response(
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = PurgeInvocationResponse;
    type Wire = PurgeInvocationRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::PurgeInvocation {
            invocation_id: self.invocation_id,
        })
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        PurgeInvocationRpcRequest {
            header,
            invocation_id: self.invocation_id,
        }
    }

    fn from_legacy_response(
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = PurgeInvocationResponse;
    type Wire = PurgeJournalRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::PurgeJournal {
            invocation_id: self.invocation_id,
        })
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        PurgeJournalRpcRequest {
            header,
            invocation_id: self.invocation_id,
        }
    }

    fn from_legacy_response(
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = RestartAsNewInvocationResponse;
    type Wire = RestartAsNewInvocationRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::RestartAsNewInvocation {
            invocation_id: self.invocation_id,
            copy_prefix_up_to_index_included: self.copy_prefix_up_to_index_included,
            patch_deployment_id: self.patch_deployment_id,
        })
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        RestartAsNewInvocationRpcRequest {
            header,
            invocation_id: self.invocation_id,
            copy_prefix_up_to_index_included: self.copy_prefix_up_to_index_included,
            patch_deployment_id: self.patch_deployment_id,
        }
    }

    fn from_legacy_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        match response? {
            PartitionProcessorRpcResponse::RestartAsNewInvocation(res) => Ok(res.try_into()?),
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = ResumeInvocationResponse;
    type Wire = ResumeInvocationRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::ResumeInvocation {
            invocation_id: self.invocation_id,
            deployment_id: self.deployment_id,
        })
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        ResumeInvocationRpcRequest {
            header,
            invocation_id: self.invocation_id,
            deployment_id: self.deployment_id,
        }
    }

    fn from_legacy_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        match response? {
            PartitionProcessorRpcResponse::ResumeInvocation(res) => Ok(res.try_into()?),
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
    const HAS_LEGACY_WIRE: bool = true;
    type Response = PauseInvocationResponse;
    type Wire = PauseInvocationRpcRequest;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        Some(PartitionProcessorRpcRequestInner::PauseInvocation {
            invocation_id: self.invocation_id,
        })
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire {
        PauseInvocationRpcRequest {
            header,
            invocation_id: self.invocation_id,
        }
    }

    fn from_legacy_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: LegacyResponse,
    ) -> Result<Self::Response, WireResponseError> {
        match response? {
            PartitionProcessorRpcResponse::PauseInvocation(res) => Ok(res.into()),
            _ => Err(WireResponseError::UnexpectedResponse),
        }
    }
}
