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
use crate::invocation::client::PauseInvocationResponse;
use crate::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcRequestHeader,
    PartitionProcessorRpcRequestInner, PartitionProcessorRpcResponse, PauseInvocationRpcRequest,
};

use super::client::{PartitionProcessorRpc, WireResponseError};

type LegacyResponse = Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>;

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
