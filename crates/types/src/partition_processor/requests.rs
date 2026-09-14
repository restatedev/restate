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
//! enum and how to lift the wire response into its typed response.

use std::sync::Arc;

use crate::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
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
    AppendInvocationReplyOn, GetInvocationOutputResponseMode, PartitionProcessorRpcRequestInner,
    PartitionProcessorRpcResponse,
};

use super::client::PartitionProcessorRpc;

/// Append the invocation to the log, replying once the partition processor emitted the
/// [`SubmittedInvocationNotification`].
#[derive(Debug, Clone)]
pub struct SubmitInvocation {
    pub invocation_request: Arc<InvocationRequest>,
}

impl PartitionProcessorRpc for SubmitInvocation {
    type Response = SubmittedInvocationNotification;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::AppendInvocation(
            self.invocation_request,
            AppendInvocationReplyOn::Submitted,
        )
    }

    fn from_response(
        request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        let PartitionProcessorRpcResponse::Submitted(submit_notification) = response else {
            return None;
        };
        debug_assert_eq!(
            request_id, submit_notification.request_id,
            "Conflicting submit notification received"
        );
        Some(submit_notification)
    }
}

/// Append the invocation to the log and wait for its output.
#[derive(Debug, Clone)]
pub struct CallInvocation {
    pub invocation_request: Arc<InvocationRequest>,
}

impl PartitionProcessorRpc for CallInvocation {
    type Response = InvocationOutput;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::AppendInvocation(
            self.invocation_request,
            AppendInvocationReplyOn::Output,
        )
    }

    fn from_response(
        request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        let PartitionProcessorRpcResponse::Output(invocation_output) = response else {
            return None;
        };
        debug_assert_eq!(
            request_id, invocation_output.request_id,
            "Conflicting invocation output received"
        );
        Some(invocation_output)
    }
}

/// Attach to an existing invocation and wait for its output.
#[derive(Debug, Clone)]
pub struct AttachInvocation {
    pub invocation_query: InvocationQuery,
}

impl PartitionProcessorRpc for AttachInvocation {
    type Response = AttachInvocationResponse;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::GetInvocationOutput(
            self.invocation_query,
            GetInvocationOutputResponseMode::BlockWhenNotReady,
        )
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        Some(match response {
            PartitionProcessorRpcResponse::NotFound => AttachInvocationResponse::NotFound,
            PartitionProcessorRpcResponse::NotSupported => AttachInvocationResponse::NotSupported,
            PartitionProcessorRpcResponse::Output(output) => {
                AttachInvocationResponse::Ready(output)
            }
            _ => return None,
        })
    }
}

/// Get an invocation output, when present.
#[derive(Debug, Clone)]
pub struct GetInvocationOutput {
    pub invocation_query: InvocationQuery,
}

impl PartitionProcessorRpc for GetInvocationOutput {
    type Response = GetInvocationOutputResponse;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::GetInvocationOutput(
            self.invocation_query,
            GetInvocationOutputResponseMode::ReplyIfNotReady,
        )
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        Some(match response {
            PartitionProcessorRpcResponse::NotFound => GetInvocationOutputResponse::NotFound,
            PartitionProcessorRpcResponse::NotSupported => {
                GetInvocationOutputResponse::NotSupported
            }
            PartitionProcessorRpcResponse::NotReady => GetInvocationOutputResponse::NotReady,
            PartitionProcessorRpcResponse::Output(output) => {
                GetInvocationOutputResponse::Ready(output)
            }
            _ => return None,
        })
    }
}

/// Get the invocation status, when present.
#[derive(Debug, Clone)]
pub struct GetInvocationStatus {
    pub invocation_id: InvocationId,
}

impl PartitionProcessorRpc for GetInvocationStatus {
    type Response = GetInvocationStatusResponse;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::GetInvocationStatus {
            invocation_id: self.invocation_id,
        }
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        Some(match response {
            PartitionProcessorRpcResponse::NotFound => GetInvocationStatusResponse::NotFound,
            PartitionProcessorRpcResponse::Status(status) => {
                GetInvocationStatusResponse::Status(status)
            }
            _ => return None,
        })
    }
}

/// **DEPRECATED** Append an [`InvocationResponse`] to an existing invocation journal.
/// Only ServiceProtocol <= 3.
#[derive(Debug, Clone)]
pub struct AppendInvocationResponse {
    pub invocation_response: InvocationResponse,
}

impl PartitionProcessorRpc for AppendInvocationResponse {
    type Response = ();

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::AppendInvocationResponse(self.invocation_response)
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        matches!(response, PartitionProcessorRpcResponse::Appended).then_some(())
    }
}

/// Append a signal to an existing invocation journal.
#[derive(Debug, Clone)]
pub struct AppendSignal {
    pub invocation_id: InvocationId,
    pub signal: Signal,
}

impl PartitionProcessorRpc for AppendSignal {
    type Response = ();

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::AppendSignal(self.invocation_id, self.signal)
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        matches!(response, PartitionProcessorRpcResponse::Appended).then_some(())
    }
}

/// Cancel the given invocation.
#[derive(Debug, Clone)]
pub struct CancelInvocation {
    pub invocation_id: InvocationId,
}

impl PartitionProcessorRpc for CancelInvocation {
    type Response = CancelInvocationResponse;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::CancelInvocation {
            invocation_id: self.invocation_id,
        }
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        match response {
            PartitionProcessorRpcResponse::CancelInvocation(res) => Some(res.into()),
            _ => None,
        }
    }
}

/// Kill the given invocation.
#[derive(Debug, Clone)]
pub struct KillInvocation {
    pub invocation_id: InvocationId,
}

impl PartitionProcessorRpc for KillInvocation {
    type Response = KillInvocationResponse;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::KillInvocation {
            invocation_id: self.invocation_id,
        }
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        match response {
            PartitionProcessorRpcResponse::KillInvocation(res) => Some(res.into()),
            _ => None,
        }
    }
}

/// Purge the given invocation. This cleanups all the state for the given invocation.
/// This command applies only to completed invocations.
#[derive(Debug, Clone)]
pub struct PurgeInvocation {
    pub invocation_id: InvocationId,
}

impl PartitionProcessorRpc for PurgeInvocation {
    type Response = PurgeInvocationResponse;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::PurgeInvocation {
            invocation_id: self.invocation_id,
        }
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        match response {
            PartitionProcessorRpcResponse::PurgeInvocation(res) => Some(res.into()),
            _ => None,
        }
    }
}

/// Purge the given invocation journal, retaining the metadata.
/// This command applies only to completed invocations.
#[derive(Debug, Clone)]
pub struct PurgeJournal {
    pub invocation_id: InvocationId,
}

impl PartitionProcessorRpc for PurgeJournal {
    type Response = PurgeInvocationResponse;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::PurgeJournal {
            invocation_id: self.invocation_id,
        }
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        match response {
            PartitionProcessorRpcResponse::PurgeJournal(res) => Some(res.into()),
            _ => None,
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

impl PartitionProcessorRpc for RestartAsNewInvocation {
    type Response = RestartAsNewInvocationResponse;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::RestartAsNewInvocation {
            invocation_id: self.invocation_id,
            copy_prefix_up_to_index_included: self.copy_prefix_up_to_index_included,
            patch_deployment_id: self.patch_deployment_id,
        }
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        match response {
            PartitionProcessorRpcResponse::RestartAsNewInvocation(res) => Some(res.into()),
            _ => None,
        }
    }
}

/// Resume the given invocation.
#[derive(Debug, Clone)]
pub struct ResumeInvocation {
    pub invocation_id: InvocationId,
    pub deployment_id: PatchDeploymentId,
}

impl PartitionProcessorRpc for ResumeInvocation {
    type Response = ResumeInvocationResponse;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::ResumeInvocation {
            invocation_id: self.invocation_id,
            deployment_id: self.deployment_id,
        }
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        match response {
            PartitionProcessorRpcResponse::ResumeInvocation(res) => Some(res.into()),
            _ => None,
        }
    }
}

/// Pause the given invocation.
#[derive(Debug, Clone)]
pub struct PauseInvocation {
    pub invocation_id: InvocationId,
}

impl PartitionProcessorRpc for PauseInvocation {
    type Response = PauseInvocationResponse;

    fn into_inner(self) -> PartitionProcessorRpcRequestInner {
        PartitionProcessorRpcRequestInner::PauseInvocation {
            invocation_id: self.invocation_id,
        }
    }

    fn from_response(
        _request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response> {
        match response {
            PartitionProcessorRpcResponse::PauseInvocation(res) => Some(res.into()),
            _ => None,
        }
    }
}
