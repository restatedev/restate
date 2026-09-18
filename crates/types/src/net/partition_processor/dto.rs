// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use bilrost::{DecodeError, DecodeErrorKind};
use bytes::Bytes;
use bytestring::ByteString;

use crate::Scope;
use crate::errors;
use crate::identifiers::IdempotencyId;
use crate::identifiers::ServiceId;
use crate::identifiers::{DeploymentId, InvocationId, PartitionProcessorRpcRequestId};
use crate::invocation::client::InvocationOutputResponse;
use crate::invocation::{self, InvocationTargetType};
use crate::time::MillisSinceEpoch;

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Oneof, bilrost::Message)]
pub enum InvocationQuery {
    #[bilrost(empty)]
    Unknown,
    #[bilrost(1)]
    Invocation(InvocationId),
    #[bilrost(2)]
    Workflow(ServiceId),
    #[bilrost(3)]
    IdempotencyId(IdempotencyId),
}

impl TryFrom<InvocationQuery> for crate::invocation::InvocationQuery {
    type Error = DecodeError;

    fn try_from(value: InvocationQuery) -> Result<Self, Self::Error> {
        Ok(match value {
            InvocationQuery::Unknown => {
                return Err(DecodeError::new(DecodeErrorKind::InvalidValue));
            }
            InvocationQuery::Invocation(id) => crate::invocation::InvocationQuery::Invocation(id),
            InvocationQuery::Workflow(id) => crate::invocation::InvocationQuery::Workflow(id),
            InvocationQuery::IdempotencyId(id) => {
                crate::invocation::InvocationQuery::IdempotencyId(id)
            }
        })
    }
}

impl From<crate::invocation::InvocationQuery> for InvocationQuery {
    fn from(value: crate::invocation::InvocationQuery) -> Self {
        match value {
            crate::invocation::InvocationQuery::Invocation(id) => InvocationQuery::Invocation(id),
            crate::invocation::InvocationQuery::Workflow(id) => InvocationQuery::Workflow(id),
            crate::invocation::InvocationQuery::IdempotencyId(id) => {
                InvocationQuery::IdempotencyId(id)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, bilrost::Enumeration)]
pub enum InvocationState {
    #[bilrost(0)]
    Scheduled,
    #[bilrost(1)]
    Inboxed,
    #[bilrost(2)]
    Invoked,
    #[bilrost(3)]
    Suspended,
    #[bilrost(4)]
    Paused,
    #[bilrost(5)]
    Killed,
    #[bilrost(6)]
    Failed,
    #[bilrost(7)]
    Succeeded,
}

impl From<InvocationState> for crate::invocation::client::InvocationState {
    fn from(value: InvocationState) -> Self {
        match value {
            InvocationState::Scheduled => crate::invocation::client::InvocationState::Scheduled,
            InvocationState::Inboxed => crate::invocation::client::InvocationState::Inboxed,
            InvocationState::Invoked => crate::invocation::client::InvocationState::Invoked,
            InvocationState::Suspended => crate::invocation::client::InvocationState::Suspended,
            InvocationState::Paused => crate::invocation::client::InvocationState::Paused,
            InvocationState::Killed => crate::invocation::client::InvocationState::Killed,
            InvocationState::Failed => crate::invocation::client::InvocationState::Failed,
            InvocationState::Succeeded => crate::invocation::client::InvocationState::Succeeded,
        }
    }
}

impl From<crate::invocation::client::InvocationState> for InvocationState {
    fn from(value: crate::invocation::client::InvocationState) -> Self {
        match value {
            crate::invocation::client::InvocationState::Scheduled => InvocationState::Scheduled,
            crate::invocation::client::InvocationState::Inboxed => InvocationState::Inboxed,
            crate::invocation::client::InvocationState::Invoked => InvocationState::Invoked,
            crate::invocation::client::InvocationState::Suspended => InvocationState::Suspended,
            crate::invocation::client::InvocationState::Paused => InvocationState::Paused,
            crate::invocation::client::InvocationState::Killed => InvocationState::Killed,
            crate::invocation::client::InvocationState::Failed => InvocationState::Failed,
            crate::invocation::client::InvocationState::Succeeded => InvocationState::Succeeded,
        }
    }
}

#[derive(Debug, Default, Clone, bilrost::Oneof, bilrost::Message, PartialEq, Eq)]
pub enum PatchDeploymentId {
    #[default]
    #[bilrost(empty)]
    KeepPinned,
    #[bilrost(tag(1), message)]
    PinToLatest,
    #[bilrost(tag(2), message)]
    PinTo {
        #[bilrost(tag(1))]
        id: DeploymentId,
    },
}

impl From<PatchDeploymentId> for crate::invocation::client::PatchDeploymentId {
    fn from(value: PatchDeploymentId) -> Self {
        match value {
            PatchDeploymentId::KeepPinned => {
                crate::invocation::client::PatchDeploymentId::KeepPinned
            }
            PatchDeploymentId::PinToLatest => {
                crate::invocation::client::PatchDeploymentId::PinToLatest
            }
            PatchDeploymentId::PinTo { id } => {
                crate::invocation::client::PatchDeploymentId::PinTo { id }
            }
        }
    }
}

impl From<crate::invocation::client::PatchDeploymentId> for PatchDeploymentId {
    fn from(value: crate::invocation::client::PatchDeploymentId) -> Self {
        match value {
            crate::invocation::client::PatchDeploymentId::KeepPinned => {
                PatchDeploymentId::KeepPinned
            }
            crate::invocation::client::PatchDeploymentId::PinToLatest => {
                PatchDeploymentId::PinToLatest
            }
            crate::invocation::client::PatchDeploymentId::PinTo { id } => {
                PatchDeploymentId::PinTo { id }
            }
        }
    }
}

#[derive(bilrost::Message)]
pub(super) struct InvocationTarget {
    #[bilrost(1)]
    name: ByteString,
    #[bilrost(2)]
    handler: ByteString,
    #[bilrost(3)]
    key: Option<ByteString>,
    #[bilrost(4)]
    ty: InvocationTargetType,
    #[bilrost(5)]
    scope: Option<Scope>,
}
impl From<invocation::InvocationTarget> for InvocationTarget {
    fn from(value: invocation::InvocationTarget) -> Self {
        match value {
            invocation::InvocationTarget::Service {
                name,
                handler,
                scope,
            } => Self {
                name,
                handler,
                key: None,
                ty: InvocationTargetType::Service,
                scope,
            },
            invocation::InvocationTarget::VirtualObject {
                name,
                key,
                handler,
                handler_ty,
                scope,
            } => Self {
                name,
                key: Some(key),
                handler,
                ty: InvocationTargetType::VirtualObject(handler_ty),
                scope,
            },
            invocation::InvocationTarget::Workflow {
                name,
                key,
                handler,
                handler_ty,
                scope,
            } => Self {
                name,
                key: Some(key),
                handler,
                ty: InvocationTargetType::Workflow(handler_ty),
                scope,
            },
        }
    }
}
impl TryFrom<InvocationTarget> for invocation::InvocationTarget {
    type Error = DecodeError;
    fn try_from(value: InvocationTarget) -> Result<Self, Self::Error> {
        let InvocationTarget {
            name,
            handler,
            key,
            ty,
            scope,
        } = value;
        Ok(match ty {
            InvocationTargetType::Service => Self::Service {
                name,
                handler,
                scope,
            },
            InvocationTargetType::VirtualObject(handler_ty) => Self::VirtualObject {
                name,
                key: key.ok_or_else(|| DecodeError::new(DecodeErrorKind::InvalidValue))?,
                handler,
                handler_ty,
                scope,
            },
            InvocationTargetType::Workflow(handler_ty) => Self::Workflow {
                name,
                key: key.ok_or_else(|| DecodeError::new(DecodeErrorKind::InvalidValue))?,
                handler,
                handler_ty,
                scope,
            },
        })
    }
}

#[derive(bilrost::Message)]
pub struct SubmittedInvocationNotification {
    #[bilrost(1)]
    request_id: PartitionProcessorRpcRequestId,
    #[bilrost(2)]
    execution_time: Option<MillisSinceEpoch>,
    #[bilrost(3)]
    is_new_invocation: bool,
}

impl From<invocation::client::SubmittedInvocationNotification> for SubmittedInvocationNotification {
    fn from(value: invocation::client::SubmittedInvocationNotification) -> Self {
        Self {
            request_id: value.request_id,
            execution_time: value.execution_time,
            is_new_invocation: value.is_new_invocation,
        }
    }
}

impl From<SubmittedInvocationNotification> for invocation::client::SubmittedInvocationNotification {
    fn from(value: SubmittedInvocationNotification) -> Self {
        Self {
            request_id: value.request_id,
            execution_time: value.execution_time,
            is_new_invocation: value.is_new_invocation,
        }
    }
}

#[derive(bilrost::Message)]
pub struct InvocationOutput {
    #[bilrost(1)]
    request_id: PartitionProcessorRpcRequestId,
    #[bilrost(2)]
    invocation_id: Option<InvocationId>,
    #[bilrost(3)]
    completion_expiry_time: Option<MillisSinceEpoch>,
    #[bilrost(oneof(4, 5))]
    response: OutputResponse,
}
#[derive(bilrost::Oneof)]
pub(super) enum OutputResponse {
    Unknown,
    #[bilrost(tag(4), message)]
    Success {
        #[bilrost(1)]
        target: InvocationTarget,
        #[bilrost(2)]
        body: Bytes,
    },
    #[bilrost(tag(5))]
    Failure(InvocationError),
}
impl From<invocation::client::InvocationOutput> for InvocationOutput {
    fn from(value: invocation::client::InvocationOutput) -> Self {
        Self {
            request_id: value.request_id,
            invocation_id: value.invocation_id,
            completion_expiry_time: value.completion_expiry_time,
            response: match value.response {
                InvocationOutputResponse::Success(target, body) => OutputResponse::Success {
                    target: target.into(),
                    body,
                },
                InvocationOutputResponse::Failure(error) => OutputResponse::Failure(error.into()),
            },
        }
    }
}
impl TryFrom<InvocationOutput> for crate::invocation::client::InvocationOutput {
    type Error = DecodeError;
    fn try_from(value: InvocationOutput) -> Result<Self, Self::Error> {
        Ok(Self {
            request_id: value.request_id,
            invocation_id: value.invocation_id,
            completion_expiry_time: value.completion_expiry_time,
            response: match value.response {
                OutputResponse::Unknown => {
                    return Err(DecodeError::new(DecodeErrorKind::InvalidValue));
                }
                OutputResponse::Success { target, body } => {
                    InvocationOutputResponse::Success(target.try_into()?, body)
                }
                OutputResponse::Failure(error) => InvocationOutputResponse::Failure(error.into()),
            },
        })
    }
}

#[derive(bilrost::Message)]
pub struct InvocationError {
    #[bilrost(1)]
    code: errors::InvocationErrorCode,
    #[bilrost(2)]
    message: String,
    #[bilrost(3)]
    stacktrace: Option<String>,
    #[bilrost(4)]
    metadata: Vec<(String, String)>,
}

impl From<InvocationError> for errors::InvocationError {
    fn from(value: InvocationError) -> Self {
        Self {
            code: value.code,
            message: Cow::Owned(value.message),
            stacktrace: value.stacktrace.map(Cow::Owned),
            metadata: value.metadata,
        }
    }
}
impl From<errors::InvocationError> for InvocationError {
    fn from(value: errors::InvocationError) -> Self {
        Self {
            code: value.code,
            message: value.message.into_owned(),
            stacktrace: value.stacktrace.map(|s| s.into_owned()),
            metadata: value.metadata,
        }
    }
}
