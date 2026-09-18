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
    CancelInvocationResponse, InvocationOutput, InvocationStatus, KillInvocationResponse,
    PatchDeploymentId, PauseInvocationResponse, PurgeInvocationResponse,
    RestartAsNewInvocationResponse, ResumeInvocationResponse, SubmittedInvocationNotification,
};
use crate::invocation::{InvocationQuery, InvocationRequest, InvocationResponse};
use crate::journal_v2::Signal;
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

/// A trait that's implemented by the envelopes of the PP RPCs. To help map the
/// envelope to its successful variant type. Needed for type magic in the handlers
/// later on. There are two envelopes right now:
/// - The legacy flexbuffers-based envelope which is a Result<PartitionProcessorRpcResponse, Error>
/// - The new bilrost-based envelope which is `PartitionProcessorResponseRpcEnvelope`.
///
/// Once the legacy envelope is removed, this trait can go away.
pub trait PartitionProcessorWireEnvelope:
    RpcResponse<Service = PartitionLeaderService>
    + From<Result<Self::Ok, PartitionProcessorRpcError>>
    + Sync
    + 'static
{
    /// The handler's successful response type.
    type Ok: Send + Sync + 'static;

    /// Converts the envelope into a result.
    fn into_result(self) -> Result<Self::Ok, WireResponseError>;
}

/// The successful payload of the wire envelope `W`.
pub type PartitionProcessorRpcOkOf<W> =
    <<W as RpcRequest>::Response as PartitionProcessorWireEnvelope>::Ok;

/// The trait implemented by all PP request wire formats.
pub trait PartitionProcessorWireRpc:
    RpcRequest<Service = PartitionLeaderService, Response: PartitionProcessorWireEnvelope>
{
    fn header(&self) -> PartitionProcessorRpcRequestHeader;
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
    fn header(&self) -> PartitionProcessorRpcRequestHeader {
        PartitionProcessorRpcRequestHeader {
            request_id: self.request_id,
            sent_at: self.sent_at,
        }
    }
}

impl From<PartitionProcessorRpcError>
    for Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>
{
    fn from(value: PartitionProcessorRpcError) -> Self {
        Err(value)
    }
}

impl PartitionProcessorWireEnvelope
    for Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>
{
    type Ok = PartitionProcessorRpcResponse;

    fn into_result(self) -> Result<PartitionProcessorRpcResponse, WireResponseError> {
        Ok(self?)
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GetInvocationOutputResponseMode {
    /// With this mode, we block waiting for the output to be ready (also known as _attach_).
    BlockWhenNotReady,
    /// With this mode, we immediately reply with [`PartitionProcessorRpcResponse::NotReady`] in case the invocation is in-flight.
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
    AppendSignal(InvocationId, Signal),
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
    Ok(T),
    #[bilrost(tag(2))]
    NotLeader(PartitionId),
    #[bilrost(tag(3))]
    LostLeadership(PartitionId),
    #[bilrost(tag(4))]
    Internal(String),
}

impl<T> PartitionProcessorWireEnvelope for PartitionProcessorResponseRpcEnvelope<T>
where
    T: Send + Sync + 'static,
    Self: RpcResponse<Service = PartitionLeaderService>,
{
    type Ok = T;

    fn into_result(self) -> Result<T, WireResponseError> {
        match self {
            Self::Ok(value) => Ok(value),
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
            Ok(value) => Self::Ok(value),
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

impl From<CancelInvocationRpcResponse> for PartitionProcessorRpcResponse {
    fn from(value: CancelInvocationRpcResponse) -> Self {
        Self::CancelInvocation(value)
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

impl From<KillInvocationRpcResponse> for PartitionProcessorRpcResponse {
    fn from(value: KillInvocationRpcResponse) -> Self {
        Self::KillInvocation(value)
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

impl From<PurgeInvocationRpcResponse> for PartitionProcessorRpcResponse {
    fn from(value: PurgeInvocationRpcResponse) -> Self {
        Self::PurgeInvocation(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestartAsNewInvocationRpcResponse {
    Ok {
        new_invocation_id: InvocationId,
    },
    NotFound,
    StillRunning,
    Unsupported,
    JournalIndexOutOfRange,
    JournalCopyRangeInvalid,
    MissingInput,
    NotStarted,
    CannotPatchDeploymentId,
    DeploymentNotFound,
    IncompatibleDeploymentId {
        pinned_protocol_version: i32,
        deployment_id: DeploymentId,
        supported_protocol_versions: RangeInclusive<i32>,
    },
}

impl From<RestartAsNewInvocationRpcResponse> for RestartAsNewInvocationResponse {
    fn from(value: RestartAsNewInvocationRpcResponse) -> Self {
        match value {
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
        }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResumeInvocationRpcResponse {
    Ok,
    NotFound,
    NotStarted,
    Completed,
    CannotPatchDeploymentId,
    DeploymentNotFound,
    IncompatibleDeploymentId {
        pinned_protocol_version: i32,
        deployment_id: DeploymentId,
        supported_protocol_versions: RangeInclusive<i32>,
    },
}

impl From<ResumeInvocationRpcResponse> for ResumeInvocationResponse {
    fn from(value: ResumeInvocationRpcResponse) -> Self {
        match value {
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
        }
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

/// TODO: Remove in 1.9 when all RPCs are usign the dedicated messages.
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
                fn header(&self) -> PartitionProcessorRpcRequestHeader {
                    self.header
                }
            }
        )*
    };
}

define_partition_processor_rpcs! {
    PauseInvocationRpcRequest => PauseInvocationRpcResponse,
}
