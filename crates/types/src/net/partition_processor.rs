// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::identifiers::{
    DeploymentId, EntryIndex, InvocationId, PartitionId, PartitionProcessorRpcRequestId,
};
use crate::invocation::client::{
    AttachInvocationResponse, CancelInvocationResponse, GetInvocationOutputResponse,
    GetInvocationStatusResponse, InvocationOutput, InvocationStatus, KillInvocationResponse,
    PatchDeploymentId, PauseInvocationResponse, PurgeInvocationResponse,
    RestartAsNewInvocationResponse, ResumeInvocationResponse, SubmittedInvocationNotification,
};
use crate::invocation::{InvocationQuery, InvocationRequest, InvocationResponse};
use crate::journal_v2;
use crate::net::codec::{
    EncodeError, WireDecode, WireEncode, decode_as_bilrost, decode_as_flexbuffers,
    encode_as_bilrost, encode_as_flexbuffers,
};
use crate::net::{
    ProtocolVersion, RpcRequest, RpcResponse, ServiceTag, bilrost_wire_codec, default_wire_codec,
    define_rpc, define_service,
};
use crate::partition_processor::client::WireResponseError;
use crate::time::MillisSinceEpoch;

mod dto;

pub struct PartitionLeaderService;

define_service! {
    @service = PartitionLeaderService,
    @tag = ServiceTag::PartitionLeaderService,
}

/// The partition processor replied with a variant the request never expects.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("unexpected response from partition processor")]
pub struct UnexpectedResponse;

/// The header that's sent with every partition processor RPC request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, bilrost::Message)]
pub struct PartitionProcessorRpcRequestHeader {
    #[bilrost(tag(1))]
    pub request_id: PartitionProcessorRpcRequestId,
    /// Time at which the source node sent the request.
    #[bilrost(tag(2))]
    pub sent_at: Option<MillisSinceEpoch>,
}

impl PartitionProcessorRpcRequestHeader {
    pub fn new(request_id: PartitionProcessorRpcRequestId) -> Self {
        Self {
            request_id,
            sent_at: Some(MillisSinceEpoch::now()),
        }
    }
}

/// The trait implemented by all PP request wire formats.
///
/// Besides the request itself, it knows how to wrap the handler's result into its wire response and
/// how to unwrap it again on the client. There are two response formats right now:
/// - The legacy flexbuffers-based `Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>`
/// - The bilrost-based [`PartitionProcessorResponseRpcEnvelope`] of the dedicated messages.
pub trait PartitionProcessorWireRpc:
    RpcRequest<Service = PartitionLeaderService, Response: Sync>
{
    /// The handler's successful response type.
    type Ok: Send + Sync + 'static;

    fn header(&self) -> PartitionProcessorRpcRequestHeader;

    /// Wraps the handler's result into the wire response.
    fn wrap_response(result: Result<Self::Ok, PartitionProcessorRpcError>) -> Self::Response;

    /// Unwraps the wire response into the handler's result.
    fn unwrap_response(response: Self::Response) -> Result<Self::Ok, WireResponseError>;
}

define_rpc! {
    @request = PartitionProcessorRpcRequest,
    @response = Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>,
    @service = PartitionLeaderService,
}

default_wire_codec!(Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>);

/// TODO: Remove in 1.9 when all RPCs are using the dedicated messages.
/// Requests to individual partition processors. We still need to route them through the PP manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionProcessorRpcRequest {
    pub request_id: PartitionProcessorRpcRequestId,
    pub partition_id: PartitionId,
    /// Time at which the source node sent the request.
    pub sent_at: Option<MillisSinceEpoch>,
    pub inner: PartitionProcessorRpcRequestInner,
}

impl WireEncode for PartitionProcessorRpcRequest {
    fn encode_to_bytes(
        &self,
        protocol_version: ProtocolVersion,
    ) -> Result<::bytes::Bytes, EncodeError> {
        // PauseInvocation required protocol version V3
        if matches!(
            &self.inner,
            PartitionProcessorRpcRequestInner::PauseInvocation { .. },
        ) && protocol_version < ProtocolVersion::V3
        {
            return Err(EncodeError::IncompatibleVersion {
                type_tag: stringify!(PartitionProcessorRpcRequest),
                min_required: ProtocolVersion::V3,
                actual: protocol_version,
            });
        }

        Ok(::bytes::Bytes::from(encode_as_flexbuffers(self)))
    }
}

impl WireDecode for PartitionProcessorRpcRequest {
    type Error = anyhow::Error;

    fn try_decode(
        buf: impl bytes::Buf,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        decode_as_flexbuffers(buf, protocol_version)
    }
}

impl PartitionProcessorRpcRequest {
    pub fn with_header(
        header: PartitionProcessorRpcRequestHeader,
        partition_id: PartitionId,
        inner: PartitionProcessorRpcRequestInner,
    ) -> Self {
        Self {
            request_id: header.request_id,
            partition_id,
            sent_at: header.sent_at,
            inner,
        }
    }
}

impl PartitionProcessorWireRpc for PartitionProcessorRpcRequest {
    type Ok = PartitionProcessorRpcResponse;

    fn header(&self) -> PartitionProcessorRpcRequestHeader {
        PartitionProcessorRpcRequestHeader {
            request_id: self.request_id,
            sent_at: self.sent_at,
        }
    }

    fn wrap_response(
        result: Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>,
    ) -> Self::Response {
        result
    }

    fn unwrap_response(
        response: Self::Response,
    ) -> Result<PartitionProcessorRpcResponse, WireResponseError> {
        Ok(response?)
    }
}

impl From<PartitionProcessorRpcError>
    for Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>
{
    fn from(value: PartitionProcessorRpcError) -> Self {
        Err(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppendInvocationReplyOn {
    /// With this mode, the PP will reply as soon as the log append is done with [`PartitionProcessorRpcResponse::Appended`].
    ///
    /// The record is appended without dedup information so it is never filtered during leadership
    /// transitions.
    Appended,
    /// With this mode, the PP will reply with the [`PartitionProcessorRpcResponse::Submitted`] when available.
    Submitted,
    /// With this mode, the PP will reply with the [`PartitionProcessorRpcResponse::Output`] when available.
    Output,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bilrost::Enumeration)]
pub enum GetInvocationOutputResponseMode {
    /// With this mode, we block waiting for the output to be ready (also known as _attach_).
    #[bilrost(0)]
    BlockWhenNotReady,
    /// With this mode, we immediately reply with [`PartitionProcessorRpcResponse::NotReady`] in case the invocation is in-flight.
    #[bilrost(1)]
    ReplyIfNotReady,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionProcessorRpcRequestInner {
    AppendInvocation(Arc<InvocationRequest>, AppendInvocationReplyOn),
    GetInvocationOutput(InvocationQuery, GetInvocationOutputResponseMode),
    GetInvocationStatus {
        invocation_id: InvocationId,
    },
    AppendInvocationResponse(InvocationResponse),
    AppendSignal(InvocationId, journal_v2::Signal),
    CancelInvocation {
        invocation_id: InvocationId,
    },
    KillInvocation {
        invocation_id: InvocationId,
    },
    PurgeInvocation {
        invocation_id: InvocationId,
    },
    PurgeJournal {
        invocation_id: InvocationId,
    },
    RestartAsNewInvocation {
        invocation_id: InvocationId,
        copy_prefix_up_to_index_included: EntryIndex,
        patch_deployment_id: PatchDeploymentId,
    },
    ResumeInvocation {
        invocation_id: InvocationId,
        deployment_id: PatchDeploymentId,
    },
    // *Since v1.6/Protocol Version V3*
    PauseInvocation {
        invocation_id: InvocationId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
pub enum PartitionProcessorRpcError {
    #[error("not leader for partition '{0}'")]
    NotLeader(PartitionId),
    #[error("not leader anymore for partition '{0}'")]
    LostLeadership(PartitionId),
    // Removed in 1.6.0. Kept here to prevent reintroduction at a later point.
    //#[error("rejecting rpc because too busy")]
    //Busy,
    #[error("internal error: {0}")]
    Internal(String),
}

/// The response envelope that wraps all the dedicated partition processor RPC messages.
/// This envelope inlines some of the common errors that can be returned by a PP RPC
/// regardless of the RPC type (e.g. getting a not a leader, etc).
#[derive(bilrost::Oneof, bilrost::Message)]
pub enum PartitionProcessorResponseRpcEnvelope<T> {
    #[bilrost(empty)]
    Unknown,
    #[bilrost(tag(1))]
    Ok { result: T },
    #[bilrost(tag(2))]
    NotLeader(PartitionId),
    #[bilrost(tag(3))]
    LostLeadership(PartitionId),
    #[bilrost(tag(4))]
    Internal(String),
}

impl<T> PartitionProcessorResponseRpcEnvelope<T> {
    /// Converts the envelope into a result.
    pub fn into_result(self) -> Result<T, WireResponseError> {
        match self {
            Self::Ok { result } => Ok(result),
            Self::NotLeader(partition_id) => {
                Err(PartitionProcessorRpcError::NotLeader(partition_id).into())
            }
            Self::LostLeadership(partition_id) => {
                Err(PartitionProcessorRpcError::LostLeadership(partition_id).into())
            }
            Self::Internal(message) => Err(PartitionProcessorRpcError::Internal(message).into()),
            Self::Unknown => Err(UnexpectedResponse.into()),
        }
    }
}

impl<T> WireEncode for PartitionProcessorResponseRpcEnvelope<T>
where
    Self: bilrost::Message,
{
    fn encode_to_bytes(
        &self,
        _protocol_version: ProtocolVersion,
    ) -> Result<::bytes::Bytes, EncodeError> {
        Ok(encode_as_bilrost(self))
    }
}

impl<T> WireDecode for PartitionProcessorResponseRpcEnvelope<T>
where
    Self: bilrost::OwnedMessage,
{
    type Error = anyhow::Error;

    fn try_decode(
        buf: impl bytes::Buf,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        decode_as_bilrost(buf, protocol_version)
    }
}

impl<T> From<Result<T, PartitionProcessorRpcError>> for PartitionProcessorResponseRpcEnvelope<T> {
    fn from(value: Result<T, PartitionProcessorRpcError>) -> Self {
        match value {
            Ok(value) => Self::Ok { result: value },
            Err(err) => err.into(),
        }
    }
}

impl<T> From<PartitionProcessorRpcError> for PartitionProcessorResponseRpcEnvelope<T> {
    fn from(value: PartitionProcessorRpcError) -> Self {
        match value {
            PartitionProcessorRpcError::NotLeader(partition_id) => Self::NotLeader(partition_id),
            PartitionProcessorRpcError::LostLeadership(partition_id) => {
                Self::LostLeadership(partition_id)
            }
            PartitionProcessorRpcError::Internal(msg) => Self::Internal(msg),
        }
    }
}

impl<T> RpcResponse for PartitionProcessorResponseRpcEnvelope<T>
where
    Self: WireDecode + WireEncode + Unpin + Send,
{
    type Service = PartitionLeaderService;
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct CancelInvocationRpcRequest {
    #[bilrost(tag(1))]
    pub header: PartitionProcessorRpcRequestHeader,
    #[bilrost(tag(2))]
    pub invocation_id: InvocationId,
}
bilrost_wire_codec!(CancelInvocationRpcRequest);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bilrost::Enumeration)]
pub enum CancelInvocationRpcResponse {
    #[bilrost(0)]
    Done,
    #[bilrost(1)]
    Appended,
    #[bilrost(2)]
    NotFound,
    #[bilrost(3)]
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

impl From<CancelInvocationRpcResponse> for PartitionProcessorRpcResponse {
    fn from(value: CancelInvocationRpcResponse) -> Self {
        Self::CancelInvocation(value)
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct KillInvocationRpcRequest {
    #[bilrost(tag(1))]
    pub header: PartitionProcessorRpcRequestHeader,
    #[bilrost(tag(2))]
    pub invocation_id: InvocationId,
}
bilrost_wire_codec!(KillInvocationRpcRequest);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bilrost::Enumeration)]
pub enum KillInvocationRpcResponse {
    #[bilrost(0)]
    Ok,
    #[bilrost(1)]
    NotFound,
    #[bilrost(2)]
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

impl From<KillInvocationRpcResponse> for PartitionProcessorRpcResponse {
    fn from(value: KillInvocationRpcResponse) -> Self {
        Self::KillInvocation(value)
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct PurgeInvocationRpcRequest {
    #[bilrost(tag(1))]
    pub header: PartitionProcessorRpcRequestHeader,
    #[bilrost(tag(2))]
    pub invocation_id: InvocationId,
}
bilrost_wire_codec!(PurgeInvocationRpcRequest);

#[derive(Debug, Clone, bilrost::Message)]
pub struct PurgeJournalRpcRequest {
    #[bilrost(tag(1))]
    pub header: PartitionProcessorRpcRequestHeader,
    #[bilrost(tag(2))]
    pub invocation_id: InvocationId,
}
bilrost_wire_codec!(PurgeJournalRpcRequest);

/// Both purge journal and purge invocation use the same response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bilrost::Enumeration)]
pub enum PurgeInvocationRpcResponse {
    #[bilrost(0)]
    Ok,
    #[bilrost(1)]
    NotFound,
    #[bilrost(2)]
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

impl From<PurgeInvocationRpcResponse> for PartitionProcessorRpcResponse {
    fn from(value: PurgeInvocationRpcResponse) -> Self {
        Self::PurgeInvocation(value)
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct RestartAsNewInvocationRpcRequest {
    #[bilrost(tag(1))]
    pub header: PartitionProcessorRpcRequestHeader,
    #[bilrost(tag(2))]
    pub invocation_id: InvocationId,
    #[bilrost(tag(3))]
    pub copy_prefix_up_to_index_included: EntryIndex,
    #[bilrost(tag(4))]
    pub patch_deployment_id: dto::PatchDeploymentId,
}

bilrost_wire_codec!(RestartAsNewInvocationRpcRequest);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bilrost::Oneof, bilrost::Message)]
pub enum RestartAsNewInvocationRpcResponse {
    #[bilrost(empty)]
    Unknown,
    #[bilrost(tag(1), message)]
    Ok {
        #[bilrost(tag(1))]
        new_invocation_id: InvocationId,
    },
    #[bilrost(tag(2), message)]
    NotFound,
    #[bilrost(tag(3), message)]
    StillRunning,
    #[bilrost(tag(4), message)]
    Unsupported,
    #[bilrost(tag(5), message)]
    JournalIndexOutOfRange,
    #[bilrost(tag(6), message)]
    JournalCopyRangeInvalid,
    #[bilrost(tag(7), message)]
    MissingInput,
    #[bilrost(tag(8), message)]
    NotStarted,
    #[bilrost(tag(9), message)]
    CannotPatchDeploymentId,
    #[bilrost(tag(10), message)]
    DeploymentNotFound,
    #[bilrost(tag(11), message)]
    IncompatibleDeploymentId {
        #[bilrost(tag(1))]
        pinned_protocol_version: i32,
        #[bilrost(tag(2))]
        deployment_id: DeploymentId,
        #[bilrost(tag(3))]
        supported_protocol_versions: RangeInclusive<i32>,
    },
}

impl TryFrom<RestartAsNewInvocationRpcResponse> for RestartAsNewInvocationResponse {
    type Error = UnexpectedResponse;

    fn try_from(value: RestartAsNewInvocationRpcResponse) -> Result<Self, Self::Error> {
        Ok(match value {
            RestartAsNewInvocationRpcResponse::Unknown => return Err(UnexpectedResponse),
            RestartAsNewInvocationRpcResponse::Ok { new_invocation_id } => {
                RestartAsNewInvocationResponse::Ok { new_invocation_id }
            }
            RestartAsNewInvocationRpcResponse::NotFound => RestartAsNewInvocationResponse::NotFound,
            RestartAsNewInvocationRpcResponse::StillRunning => {
                RestartAsNewInvocationResponse::StillRunning
            }
            RestartAsNewInvocationRpcResponse::Unsupported => {
                RestartAsNewInvocationResponse::Unsupported
            }
            RestartAsNewInvocationRpcResponse::MissingInput => {
                RestartAsNewInvocationResponse::MissingInput
            }
            RestartAsNewInvocationRpcResponse::NotStarted => {
                RestartAsNewInvocationResponse::NotStarted
            }
            RestartAsNewInvocationRpcResponse::JournalIndexOutOfRange => {
                RestartAsNewInvocationResponse::JournalIndexOutOfRange
            }
            RestartAsNewInvocationRpcResponse::JournalCopyRangeInvalid => {
                RestartAsNewInvocationResponse::JournalCopyRangeInvalid
            }
            RestartAsNewInvocationRpcResponse::CannotPatchDeploymentId => {
                RestartAsNewInvocationResponse::CannotPatchDeploymentId
            }
            RestartAsNewInvocationRpcResponse::DeploymentNotFound => {
                RestartAsNewInvocationResponse::DeploymentNotFound
            }
            RestartAsNewInvocationRpcResponse::IncompatibleDeploymentId {
                deployment_id,
                supported_protocol_versions,
                pinned_protocol_version,
            } => RestartAsNewInvocationResponse::IncompatibleDeploymentId {
                pinned_protocol_version,
                deployment_id,
                supported_protocol_versions,
            },
        })
    }
}

impl From<RestartAsNewInvocationResponse> for RestartAsNewInvocationRpcResponse {
    fn from(value: RestartAsNewInvocationResponse) -> Self {
        match value {
            RestartAsNewInvocationResponse::Ok { new_invocation_id } => {
                RestartAsNewInvocationRpcResponse::Ok { new_invocation_id }
            }
            RestartAsNewInvocationResponse::NotFound => RestartAsNewInvocationRpcResponse::NotFound,
            RestartAsNewInvocationResponse::StillRunning => {
                RestartAsNewInvocationRpcResponse::StillRunning
            }
            RestartAsNewInvocationResponse::Unsupported => {
                RestartAsNewInvocationRpcResponse::Unsupported
            }
            RestartAsNewInvocationResponse::MissingInput => {
                RestartAsNewInvocationRpcResponse::MissingInput
            }
            RestartAsNewInvocationResponse::NotStarted => {
                RestartAsNewInvocationRpcResponse::NotStarted
            }
            RestartAsNewInvocationResponse::JournalIndexOutOfRange => {
                RestartAsNewInvocationRpcResponse::JournalIndexOutOfRange
            }
            RestartAsNewInvocationResponse::JournalCopyRangeInvalid => {
                RestartAsNewInvocationRpcResponse::JournalCopyRangeInvalid
            }
            RestartAsNewInvocationResponse::CannotPatchDeploymentId => {
                RestartAsNewInvocationRpcResponse::CannotPatchDeploymentId
            }
            RestartAsNewInvocationResponse::DeploymentNotFound => {
                RestartAsNewInvocationRpcResponse::DeploymentNotFound
            }
            RestartAsNewInvocationResponse::IncompatibleDeploymentId {
                deployment_id,
                supported_protocol_versions,
                pinned_protocol_version,
            } => RestartAsNewInvocationRpcResponse::IncompatibleDeploymentId {
                pinned_protocol_version,
                deployment_id,
                supported_protocol_versions,
            },
        }
    }
}

impl From<RestartAsNewInvocationRpcResponse> for PartitionProcessorRpcResponse {
    fn from(value: RestartAsNewInvocationRpcResponse) -> Self {
        Self::RestartAsNewInvocation(value)
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct ResumeInvocationRpcRequest {
    #[bilrost(tag(1))]
    pub header: PartitionProcessorRpcRequestHeader,
    #[bilrost(tag(2))]
    pub invocation_id: InvocationId,
    #[bilrost(tag(3))]
    pub deployment_id: dto::PatchDeploymentId,
}
bilrost_wire_codec!(ResumeInvocationRpcRequest);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bilrost::Oneof, bilrost::Message)]
pub enum ResumeInvocationRpcResponse {
    #[bilrost(empty)]
    Unknown,
    #[bilrost(tag(1), message)]
    Ok,
    #[bilrost(tag(2), message)]
    NotFound,
    #[bilrost(tag(3), message)]
    NotStarted,
    #[bilrost(tag(4), message)]
    Completed,
    #[bilrost(tag(5), message)]
    CannotPatchDeploymentId,
    #[bilrost(tag(6), message)]
    DeploymentNotFound,
    #[bilrost(tag(7), message)]
    IncompatibleDeploymentId {
        #[bilrost(tag(1))]
        pinned_protocol_version: i32,
        #[bilrost(tag(2))]
        deployment_id: DeploymentId,
        #[bilrost(tag(3))]
        supported_protocol_versions: RangeInclusive<i32>,
    },
}

impl TryFrom<ResumeInvocationRpcResponse> for ResumeInvocationResponse {
    type Error = UnexpectedResponse;

    fn try_from(value: ResumeInvocationRpcResponse) -> Result<Self, Self::Error> {
        Ok(match value {
            ResumeInvocationRpcResponse::Unknown => return Err(UnexpectedResponse),
            ResumeInvocationRpcResponse::Ok => ResumeInvocationResponse::Ok,
            ResumeInvocationRpcResponse::NotFound => ResumeInvocationResponse::NotFound,
            ResumeInvocationRpcResponse::NotStarted => ResumeInvocationResponse::NotStarted,
            ResumeInvocationRpcResponse::Completed => ResumeInvocationResponse::Completed,
            ResumeInvocationRpcResponse::CannotPatchDeploymentId => {
                ResumeInvocationResponse::CannotChangeDeploymentId
            }
            ResumeInvocationRpcResponse::DeploymentNotFound => {
                ResumeInvocationResponse::DeploymentNotFound
            }
            ResumeInvocationRpcResponse::IncompatibleDeploymentId {
                pinned_protocol_version,
                deployment_id,
                supported_protocol_versions,
            } => ResumeInvocationResponse::IncompatibleDeploymentId {
                pinned_protocol_version,
                deployment_id,
                supported_protocol_versions,
            },
        })
    }
}

impl From<ResumeInvocationResponse> for ResumeInvocationRpcResponse {
    fn from(value: ResumeInvocationResponse) -> Self {
        match value {
            ResumeInvocationResponse::Ok => ResumeInvocationRpcResponse::Ok,
            ResumeInvocationResponse::NotFound => ResumeInvocationRpcResponse::NotFound,
            ResumeInvocationResponse::NotStarted => ResumeInvocationRpcResponse::NotStarted,
            ResumeInvocationResponse::Completed => ResumeInvocationRpcResponse::Completed,
            ResumeInvocationResponse::CannotChangeDeploymentId => {
                ResumeInvocationRpcResponse::CannotPatchDeploymentId
            }
            ResumeInvocationResponse::DeploymentNotFound => {
                ResumeInvocationRpcResponse::DeploymentNotFound
            }
            ResumeInvocationResponse::IncompatibleDeploymentId {
                pinned_protocol_version,
                deployment_id,
                supported_protocol_versions,
            } => ResumeInvocationRpcResponse::IncompatibleDeploymentId {
                pinned_protocol_version,
                deployment_id,
                supported_protocol_versions,
            },
        }
    }
}

impl From<ResumeInvocationRpcResponse> for PartitionProcessorRpcResponse {
    fn from(value: ResumeInvocationRpcResponse) -> Self {
        Self::ResumeInvocation(value)
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct PauseInvocationRpcRequest {
    #[bilrost(tag(1))]
    pub header: PartitionProcessorRpcRequestHeader,
    #[bilrost(tag(2))]
    pub invocation_id: InvocationId,
}
bilrost_wire_codec!(PauseInvocationRpcRequest);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bilrost::Enumeration)]
pub enum PauseInvocationRpcResponse {
    #[bilrost(0)]
    AlreadyPaused,
    #[bilrost(1)]
    Accepted,
    #[bilrost(2)]
    NotFound,
    #[bilrost(3)]
    NotRunning,
}

impl From<PauseInvocationRpcResponse> for PauseInvocationResponse {
    fn from(value: PauseInvocationRpcResponse) -> Self {
        match value {
            PauseInvocationRpcResponse::Accepted => PauseInvocationResponse::Accepted,
            PauseInvocationRpcResponse::NotFound => PauseInvocationResponse::NotFound,
            PauseInvocationRpcResponse::NotRunning => PauseInvocationResponse::NotRunning,
            PauseInvocationRpcResponse::AlreadyPaused => PauseInvocationResponse::AlreadyPaused,
        }
    }
}

impl From<PauseInvocationResponse> for PauseInvocationRpcResponse {
    fn from(value: PauseInvocationResponse) -> Self {
        match value {
            PauseInvocationResponse::Accepted => PauseInvocationRpcResponse::Accepted,
            PauseInvocationResponse::NotFound => PauseInvocationRpcResponse::NotFound,
            PauseInvocationResponse::NotRunning => PauseInvocationRpcResponse::NotRunning,
            PauseInvocationResponse::AlreadyPaused => PauseInvocationRpcResponse::AlreadyPaused,
        }
    }
}

impl From<PauseInvocationRpcResponse> for PartitionProcessorRpcResponse {
    fn from(value: PauseInvocationRpcResponse) -> Self {
        Self::PauseInvocation(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendInvocationRpcRequest {
    pub header: PartitionProcessorRpcRequestHeader,
    pub invocation_request: Arc<InvocationRequest>,
    pub append_invocation_reply_on: AppendInvocationReplyOn,
}
// AppendInvocationRpcRequest is still flexbuffers encoded because converting it is a bit involved.
// TODO: Convert this request to bilrost.
default_wire_codec!(AppendInvocationRpcRequest);

#[allow(clippy::large_enum_variant)]
#[derive(bilrost::Oneof, bilrost::Message)]
pub enum AppendInvocationRpcResponse {
    #[bilrost(empty)]
    Unknown,
    #[bilrost(tag(1), message)]
    Appended,
    #[bilrost(tag(2), message)]
    Submitted(dto::SubmittedInvocationNotification),
    #[bilrost(tag(3), message)]
    Output(dto::InvocationOutput),
}

impl TryFrom<AppendInvocationRpcResponse> for PartitionProcessorRpcResponse {
    type Error = UnexpectedResponse;

    fn try_from(value: AppendInvocationRpcResponse) -> Result<Self, Self::Error> {
        Ok(match value {
            AppendInvocationRpcResponse::Unknown => return Err(UnexpectedResponse),
            AppendInvocationRpcResponse::Appended => Self::Appended,
            AppendInvocationRpcResponse::Submitted(notification) => {
                Self::Submitted(notification.into())
            }
            AppendInvocationRpcResponse::Output(output) => {
                Self::Output(output.try_into().map_err(|_| UnexpectedResponse)?)
            }
        })
    }
}

impl TryFrom<AppendInvocationRpcResponse> for SubmittedInvocationNotification {
    type Error = UnexpectedResponse;
    fn try_from(value: AppendInvocationRpcResponse) -> Result<Self, Self::Error> {
        match value {
            AppendInvocationRpcResponse::Submitted(notification) => Ok(notification.into()),
            _ => Err(UnexpectedResponse),
        }
    }
}

impl TryFrom<AppendInvocationRpcResponse> for InvocationOutput {
    type Error = UnexpectedResponse;
    fn try_from(value: AppendInvocationRpcResponse) -> Result<Self, Self::Error> {
        match value {
            AppendInvocationRpcResponse::Output(output) => {
                Ok(output.try_into().map_err(|_| UnexpectedResponse)?)
            }
            _ => Err(UnexpectedResponse),
        }
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct GetInvocationOutputRpcRequest {
    #[bilrost(1)]
    pub header: PartitionProcessorRpcRequestHeader,
    #[bilrost(2)]
    pub invocation_query: dto::InvocationQuery,
    #[bilrost(3)]
    pub response_mode: GetInvocationOutputResponseMode,
}
bilrost_wire_codec!(GetInvocationOutputRpcRequest);

#[allow(clippy::large_enum_variant)]
#[derive(bilrost::Oneof, bilrost::Message)]
pub enum GetInvocationOutputRpcResponse {
    #[bilrost(empty)]
    Unknown,
    #[bilrost(tag(1), message)]
    NotFound,
    #[bilrost(tag(2), message)]
    NotReady,
    #[bilrost(tag(3), message)]
    NotSupported,
    #[bilrost(tag(4), message)]
    Output(dto::InvocationOutput),
}

impl TryFrom<GetInvocationOutputRpcResponse> for PartitionProcessorRpcResponse {
    type Error = UnexpectedResponse;

    fn try_from(value: GetInvocationOutputRpcResponse) -> Result<Self, Self::Error> {
        Ok(match value {
            GetInvocationOutputRpcResponse::Unknown => return Err(UnexpectedResponse),
            GetInvocationOutputRpcResponse::NotFound => Self::NotFound,
            GetInvocationOutputRpcResponse::NotReady => Self::NotReady,
            GetInvocationOutputRpcResponse::NotSupported => Self::NotSupported,
            GetInvocationOutputRpcResponse::Output(output) => {
                Self::Output(output.try_into().map_err(|_| UnexpectedResponse)?)
            }
        })
    }
}

impl TryFrom<GetInvocationOutputRpcResponse> for GetInvocationOutputResponse {
    type Error = UnexpectedResponse;

    fn try_from(value: GetInvocationOutputRpcResponse) -> Result<Self, Self::Error> {
        Ok(match value {
            GetInvocationOutputRpcResponse::Unknown => return Err(UnexpectedResponse),
            GetInvocationOutputRpcResponse::NotFound => Self::NotFound,
            GetInvocationOutputRpcResponse::NotReady => Self::NotReady,
            GetInvocationOutputRpcResponse::NotSupported => Self::NotSupported,
            GetInvocationOutputRpcResponse::Output(output) => {
                Self::Ready(output.try_into().map_err(|_| UnexpectedResponse)?)
            }
        })
    }
}
/// Attaching blocks until the output is ready, so `NotReady` is never a valid reply.
impl TryFrom<GetInvocationOutputRpcResponse> for AttachInvocationResponse {
    type Error = UnexpectedResponse;
    fn try_from(value: GetInvocationOutputRpcResponse) -> Result<Self, Self::Error> {
        Ok(match value {
            GetInvocationOutputRpcResponse::Unknown => return Err(UnexpectedResponse),
            GetInvocationOutputRpcResponse::NotFound => Self::NotFound,
            GetInvocationOutputRpcResponse::NotSupported => Self::NotSupported,
            GetInvocationOutputRpcResponse::Output(output) => {
                Self::Ready(output.try_into().map_err(|_| UnexpectedResponse)?)
            }
            GetInvocationOutputRpcResponse::NotReady => return Err(UnexpectedResponse),
        })
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct GetInvocationStatusRpcRequest {
    #[bilrost(tag(1))]
    pub header: PartitionProcessorRpcRequestHeader,
    #[bilrost(tag(2))]
    pub invocation_id: InvocationId,
}
bilrost_wire_codec!(GetInvocationStatusRpcRequest);

#[derive(bilrost::Oneof, bilrost::Message)]
pub enum GetInvocationStatusRpcResponse {
    #[bilrost(empty)]
    Unknown,
    #[bilrost(tag(1), message)]
    NotFound,
    #[bilrost(tag(2), message)]
    Status {
        #[bilrost(1)]
        state: dto::InvocationState,
        #[bilrost(2)]
        error: Option<dto::InvocationError>,
    },
}

impl TryFrom<GetInvocationStatusRpcResponse> for PartitionProcessorRpcResponse {
    type Error = UnexpectedResponse;

    fn try_from(value: GetInvocationStatusRpcResponse) -> Result<Self, Self::Error> {
        Ok(match value {
            GetInvocationStatusRpcResponse::Unknown => return Err(UnexpectedResponse),
            GetInvocationStatusRpcResponse::NotFound => Self::NotFound,
            GetInvocationStatusRpcResponse::Status { state, error } => {
                Self::Status(InvocationStatus {
                    state: state.into(),
                    error: error.map(Into::into),
                })
            }
        })
    }
}

impl TryFrom<GetInvocationStatusRpcResponse> for GetInvocationStatusResponse {
    type Error = UnexpectedResponse;

    fn try_from(value: GetInvocationStatusRpcResponse) -> Result<Self, Self::Error> {
        Ok(match value {
            GetInvocationStatusRpcResponse::Unknown => return Err(UnexpectedResponse),
            GetInvocationStatusRpcResponse::NotFound => Self::NotFound,
            GetInvocationStatusRpcResponse::Status { state, error } => {
                Self::Status(InvocationStatus {
                    state: state.into(),
                    error: error.map(Into::into),
                })
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendInvocationResponseRpcRequest {
    pub header: PartitionProcessorRpcRequestHeader,
    pub invocation_response: InvocationResponse,
}
// AppendInvocationResponseRpcRequest is deprecated, as such converting it to bilrost is not
// worth the effort specially since it's non-trivial.
default_wire_codec!(AppendInvocationResponseRpcRequest);

#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Message)]
pub struct AppendInvocationResponseRpcResponse;

impl From<AppendInvocationResponseRpcResponse> for PartitionProcessorRpcResponse {
    fn from(_: AppendInvocationResponseRpcResponse) -> Self {
        Self::Appended
    }
}

#[derive(Debug, Clone, bilrost::Oneof)]
pub enum SignalId {
    #[bilrost(tag(3))]
    Index(u32),
    #[bilrost(tag(4))]
    Name(bytestring::ByteString),
}

impl From<journal_v2::SignalId> for SignalId {
    fn from(value: journal_v2::SignalId) -> Self {
        match value {
            journal_v2::SignalId::Index(idx) => SignalId::Index(idx),
            journal_v2::SignalId::Name(name) => SignalId::Name(name),
        }
    }
}

impl From<SignalId> for journal_v2::SignalId {
    fn from(value: SignalId) -> Self {
        match value {
            SignalId::Index(idx) => journal_v2::SignalId::Index(idx),
            SignalId::Name(name) => journal_v2::SignalId::Name(name),
        }
    }
}

#[derive(Debug, Clone, bilrost::Oneof)]
pub enum SignalResult {
    #[bilrost(tag(5), message)]
    Void,
    #[bilrost(tag(6))]
    Success(bytes::Bytes),
    #[bilrost(tag(7))]
    Failure(journal_v2::Failure),
}

impl From<journal_v2::SignalResult> for SignalResult {
    fn from(value: journal_v2::SignalResult) -> Self {
        match value {
            journal_v2::SignalResult::Void => SignalResult::Void,
            journal_v2::SignalResult::Success(s) => SignalResult::Success(s),
            journal_v2::SignalResult::Failure(f) => SignalResult::Failure(f),
        }
    }
}

impl From<SignalResult> for journal_v2::SignalResult {
    fn from(value: SignalResult) -> Self {
        match value {
            SignalResult::Void => journal_v2::SignalResult::Void,
            SignalResult::Success(s) => journal_v2::SignalResult::Success(s),
            SignalResult::Failure(f) => journal_v2::SignalResult::Failure(f),
        }
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct AppendSignalRpcRequest {
    #[bilrost(1)]
    pub header: PartitionProcessorRpcRequestHeader,
    #[bilrost(2)]
    pub invocation_id: InvocationId,
    #[bilrost(oneof(3, 4))]
    pub signal_id: Option<SignalId>,
    #[bilrost(oneof(5, 6, 7))]
    pub result: Option<SignalResult>,
}
bilrost_wire_codec!(AppendSignalRpcRequest);

#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Message)]
pub struct AppendSignalRpcResponse;

impl From<AppendSignalRpcResponse> for PartitionProcessorRpcResponse {
    fn from(_: AppendSignalRpcResponse) -> Self {
        Self::Appended
    }
}

/// TODO: Remove in 1.9 when all RPCs are using the dedicated messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionProcessorRpcResponse {
    Appended,
    NotFound,
    NotReady,
    NotSupported,
    Submitted(SubmittedInvocationNotification),
    Output(InvocationOutput),
    Status(InvocationStatus),
    CancelInvocation(CancelInvocationRpcResponse),
    KillInvocation(KillInvocationRpcResponse),
    PurgeInvocation(PurgeInvocationRpcResponse),
    PurgeJournal(PurgeInvocationRpcResponse),
    RestartAsNewInvocation(RestartAsNewInvocationRpcResponse),
    ResumeInvocation(ResumeInvocationRpcResponse),
    PauseInvocation(PauseInvocationRpcResponse),
}

/// Registers dedicated partition processor rpcs: `Request => Ok` means the processor answers
/// `Request` with `Ok`. The envelope is fixed for all new RPCs.
macro_rules! define_partition_processor_rpcs {
    ($($request:ty => $ok:ty),* $(,)?) => {
        $(
            impl RpcRequest for $request {
                const TYPE: &str = stringify!($request);
                type Response = PartitionProcessorResponseRpcEnvelope<$ok>;
                type Service = PartitionLeaderService;
            }
            impl PartitionProcessorWireRpc for $request {
                type Ok = $ok;

                fn header(&self) -> PartitionProcessorRpcRequestHeader {
                    self.header
                }

                fn wrap_response(result: Result<$ok, PartitionProcessorRpcError>) -> Self::Response {
                    result.into()
                }

                fn unwrap_response(response: Self::Response) -> Result<$ok, WireResponseError> {
                    response.into_result()
                }
            }
        )*
    };
}

define_partition_processor_rpcs! {
    AppendInvocationRpcRequest => AppendInvocationRpcResponse,
    GetInvocationOutputRpcRequest => GetInvocationOutputRpcResponse,
    GetInvocationStatusRpcRequest => GetInvocationStatusRpcResponse,
    AppendInvocationResponseRpcRequest => AppendInvocationResponseRpcResponse,
    AppendSignalRpcRequest => AppendSignalRpcResponse,
    CancelInvocationRpcRequest => CancelInvocationRpcResponse,
    KillInvocationRpcRequest => KillInvocationRpcResponse,
    PurgeInvocationRpcRequest => PurgeInvocationRpcResponse,
    PurgeJournalRpcRequest => PurgeInvocationRpcResponse,
    RestartAsNewInvocationRpcRequest => RestartAsNewInvocationRpcResponse,
    ResumeInvocationRpcRequest => ResumeInvocationRpcResponse,
    PauseInvocationRpcRequest => PauseInvocationRpcResponse,
}
