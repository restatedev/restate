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
use crate::invocation::client::{InvocationOutput, SubmittedInvocationNotification};
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
}

impl WithPartitionKey for PartitionProcessorRpcRequestInner {
    fn partition_key(&self) -> PartitionKey {
        match self {
            PartitionProcessorRpcRequestInner::AppendInvocation(si, _) => si.partition_key(),
            PartitionProcessorRpcRequestInner::GetInvocationOutput(iq, _) => iq.partition_key(),
            PartitionProcessorRpcRequestInner::AppendInvocationResponse(ir) => ir.partition_key(),
            PartitionProcessorRpcRequestInner::AppendSignal(si, _) => si.partition_key(),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionProcessorRpcResponse {
    Appended,
    NotFound,
    NotReady,
    NotSupported,
    Submitted(SubmittedInvocationNotification),
    Output(InvocationOutput),
}
