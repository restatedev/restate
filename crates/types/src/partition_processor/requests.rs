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

use crate::identifiers::{
    InvocationId, PartitionKey, PartitionProcessorRpcRequestId, WithPartitionKey,
};
use crate::invocation::InvocationResponse;
use crate::invocation::client::{
    CancelInvocationResponse, KillInvocationResponse, PatchDeploymentId, PauseInvocationResponse,
    PurgeInvocationResponse, RestartAsNewInvocationResponse, ResumeInvocationResponse,
};
use crate::journal::EntryIndex;
use crate::journal_v2::Signal;
use crate::net::partition_processor::{
    AppendInvocationResponseRpcRequest, AppendInvocationResponseRpcResponse,
    AppendSignalRpcRequest, AppendSignalRpcResponse, CancelInvocationRpcRequest,
    KillInvocationRpcRequest, PartitionProcessorRpcError, PartitionProcessorRpcRequestHeader,
    PartitionProcessorRpcRequestInner, PartitionProcessorRpcResponse, PauseInvocationRpcRequest,
    PurgeInvocationRpcRequest, PurgeJournalRpcRequest, RestartAsNewInvocationRpcRequest,
    ResumeInvocationRpcRequest,
};

use super::client::{PartitionProcessorRpc, WireResponseError};

type LegacyResponse = Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>;

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
            patch_deployment_id: self.patch_deployment_id.into(),
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
            deployment_id: self.deployment_id.into(),
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
