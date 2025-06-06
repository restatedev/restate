// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::{
    InvocationId, PartitionId, PartitionKey, PartitionProcessorRpcRequestId, WithPartitionKey,
};
use crate::invocation::client::{
    CancelInvocationResponse, InvocationOutput, KillInvocationResponse, PurgeInvocationResponse,
    SubmittedInvocationNotification,
};
use crate::invocation::{InvocationQuery, InvocationRequest, InvocationResponse};
use crate::journal_v2::Signal;
use crate::net::ServiceTag;
use crate::net::{default_wire_codec, define_rpc, define_service};
use serde::{Deserialize, Serialize};

pub struct PartitionLeaderService;

define_service! {
    @service = PartitionLeaderService,
    @tag = ServiceTag::PartitionLeaderService,
}

define_rpc! {
    @request = PartitionProcessorRpcRequest,
    @response = Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>,
    @service = PartitionLeaderService,
}
default_wire_codec!(PartitionProcessorRpcRequest);
default_wire_codec!(Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>);

/// Requests to individual partition processors. We still need to route them through the PP manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionProcessorRpcRequest {
    pub request_id: PartitionProcessorRpcRequestId,
    pub partition_id: PartitionId,
    pub inner: PartitionProcessorRpcRequestInner,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppendInvocationReplyOn {
    /// With this mode, the PP will reply as soon as the log append is done with [`PartitionProcessorRpcResponse::Appended`].
    Appended,
    /// With this mode, the PP will reply with the [`PartitionProcessorRpcResponse::Submitted`] when available.
    Submitted,
    /// With this mode, the PP will reply with the [`PartitionProcessorRpcResponse::Output`] when available.
    Output,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GetInvocationOutputResponseMode {
    /// With this mode, we block waiting for the output to be ready (also known as _attach_).
    BlockWhenNotReady,
    /// With this mode, we immediately reply with [`PartitionProcessorRpcResponse::NotReady`] in case the invocation is in-flight.
    ReplyIfNotReady,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionProcessorRpcRequestInner {
    AppendInvocation(InvocationRequest, AppendInvocationReplyOn),
    GetInvocationOutput(InvocationQuery, GetInvocationOutputResponseMode),
    AppendInvocationResponse(InvocationResponse),
    AppendSignal(InvocationId, Signal),
    CancelInvocation { invocation_id: InvocationId },
    KillInvocation { invocation_id: InvocationId },
    PurgeInvocation { invocation_id: InvocationId },
    PurgeJournal { invocation_id: InvocationId },
}

impl WithPartitionKey for PartitionProcessorRpcRequestInner {
    fn partition_key(&self) -> PartitionKey {
        match self {
            PartitionProcessorRpcRequestInner::AppendInvocation(si, _) => si.partition_key(),
            PartitionProcessorRpcRequestInner::GetInvocationOutput(iq, _) => iq.partition_key(),
            PartitionProcessorRpcRequestInner::AppendInvocationResponse(ir) => ir.partition_key(),
            PartitionProcessorRpcRequestInner::AppendSignal(si, _) => si.partition_key(),
            PartitionProcessorRpcRequestInner::CancelInvocation { invocation_id } => {
                invocation_id.partition_key()
            }
            PartitionProcessorRpcRequestInner::KillInvocation { invocation_id } => {
                invocation_id.partition_key()
            }
            PartitionProcessorRpcRequestInner::PurgeInvocation { invocation_id } => {
                invocation_id.partition_key()
            }
            PartitionProcessorRpcRequestInner::PurgeJournal { invocation_id } => {
                invocation_id.partition_key()
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum PartitionProcessorRpcError {
    #[error("not leader for partition '{0}'")]
    NotLeader(PartitionId),
    #[error("not leader anymore for partition '{0}'")]
    LostLeadership(PartitionId),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("partition processor starting")]
    Starting,
    #[error("partition processor stopping")]
    Stopping,
}

impl PartitionProcessorRpcError {
    pub fn likely_stale_route(&self) -> bool {
        match self {
            PartitionProcessorRpcError::NotLeader(_) => true,
            PartitionProcessorRpcError::LostLeadership(_) => true,
            PartitionProcessorRpcError::Stopping => true,
            PartitionProcessorRpcError::Internal(_) => false,
            PartitionProcessorRpcError::Starting => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelInvocationRpcResponse {
    Done,
    Appended,
    NotFound,
    AlreadyCompleted,
}

impl From<CancelInvocationRpcResponse> for CancelInvocationResponse {
    fn from(value: CancelInvocationRpcResponse) -> Self {
        match value {
            CancelInvocationRpcResponse::Done => Self::Done,
            CancelInvocationRpcResponse::Appended => Self::Appended,
            CancelInvocationRpcResponse::NotFound => Self::NotFound,
            CancelInvocationRpcResponse::AlreadyCompleted => Self::AlreadyCompleted,
        }
    }
}

impl From<CancelInvocationResponse> for CancelInvocationRpcResponse {
    fn from(value: CancelInvocationResponse) -> Self {
        match value {
            CancelInvocationResponse::Done => Self::Done,
            CancelInvocationResponse::Appended => Self::Appended,
            CancelInvocationResponse::NotFound => Self::NotFound,
            CancelInvocationResponse::AlreadyCompleted => Self::AlreadyCompleted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KillInvocationRpcResponse {
    Ok,
    NotFound,
    AlreadyCompleted,
}

impl From<KillInvocationRpcResponse> for KillInvocationResponse {
    fn from(value: KillInvocationRpcResponse) -> Self {
        match value {
            KillInvocationRpcResponse::Ok => Self::Ok,
            KillInvocationRpcResponse::NotFound => Self::NotFound,
            KillInvocationRpcResponse::AlreadyCompleted => Self::AlreadyCompleted,
        }
    }
}

impl From<KillInvocationResponse> for KillInvocationRpcResponse {
    fn from(value: KillInvocationResponse) -> Self {
        match value {
            KillInvocationResponse::Ok => Self::Ok,
            KillInvocationResponse::NotFound => Self::NotFound,
            KillInvocationResponse::AlreadyCompleted => Self::AlreadyCompleted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PurgeInvocationRpcResponse {
    Ok,
    NotFound,
    NotCompleted,
}

impl From<PurgeInvocationRpcResponse> for PurgeInvocationResponse {
    fn from(value: PurgeInvocationRpcResponse) -> Self {
        match value {
            PurgeInvocationRpcResponse::Ok => Self::Ok,
            PurgeInvocationRpcResponse::NotFound => Self::NotFound,
            PurgeInvocationRpcResponse::NotCompleted => Self::NotCompleted,
        }
    }
}

impl From<PurgeInvocationResponse> for PurgeInvocationRpcResponse {
    fn from(value: PurgeInvocationResponse) -> Self {
        match value {
            PurgeInvocationResponse::Ok => Self::Ok,
            PurgeInvocationResponse::NotFound => Self::NotFound,
            PurgeInvocationResponse::NotCompleted => Self::NotCompleted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionProcessorRpcResponse {
    Appended,
    NotFound,
    NotReady,
    NotSupported,
    Submitted(SubmittedInvocationNotification),
    Output(InvocationOutput),
    CancelInvocation(CancelInvocationRpcResponse),
    KillInvocation(KillInvocationRpcResponse),
    PurgeInvocation(PurgeInvocationRpcResponse),
    PurgeJournal(PurgeInvocationRpcResponse),
}
