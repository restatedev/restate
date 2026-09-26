// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;

use crate::identifiers::{PartitionProcessorRpcRequestId, WithPartitionKey};
use crate::net::RpcRequest;
use crate::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcRequestHeader,
    PartitionProcessorRpcRequestInner, PartitionProcessorRpcResponse, PartitionProcessorWireRpc,
    UnexpectedResponse,
};

#[derive(Debug, thiserror::Error)]
#[error("{inner}")]
pub struct PartitionProcessorClientError {
    is_safe_to_retry: bool,
    #[source]
    inner: anyhow::Error,
}

impl PartitionProcessorClientError {
    pub fn new(inner: impl Into<anyhow::Error>, is_safe_to_retry: bool) -> Self {
        Self {
            is_safe_to_retry,
            inner: inner.into(),
        }
    }

    pub fn is_safe_to_retry(&self) -> bool {
        self.is_safe_to_retry
    }

    pub fn into_inner(self) -> anyhow::Error {
        self.inner
    }
}

/// Error lifting a wire response into a typed response.
#[derive(Debug, thiserror::Error)]
pub enum WireResponseError {
    #[error(transparent)]
    Processor(#[from] PartitionProcessorRpcError),
    /// The partition processor replied with a variant the request never expects.
    #[error("unexpected response from partition processor")]
    UnexpectedResponse,
}

impl From<UnexpectedResponse> for WireResponseError {
    fn from(_: UnexpectedResponse) -> Self {
        Self::UnexpectedResponse
    }
}

impl From<Infallible> for WireResponseError {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

/// A typed request to a partition processor.
///
/// The wire reply is lifted into [`Self::Response`] with [`TryFrom`], so a request whose response
/// is the wire payload itself, or has a `From`/`TryFrom` impl for it, only implements
/// [`Self::into_wire`]. Requests without a legacy form leave the legacy methods at their defaults.
pub trait PartitionProcessorRpc: WithPartitionKey + Sized + Send + 'static {
    /// Whether this RPC has a legacy wire implementation or not.
    const HAS_LEGACY_WIRE: bool = false;

    /// The wire message sent over the network.
    type Wire: PartitionProcessorWireRpc;

    /// The typed response the caller receives.
    type Response: TryFrom<<Self::Wire as PartitionProcessorWireRpc>::Ok, Error: Into<WireResponseError>>
        + Send;

    fn into_legacy_wire(self) -> Option<PartitionProcessorRpcRequestInner> {
        None
    }

    fn from_legacy_response(
        _request_id: PartitionProcessorRpcRequestId,
        _response: Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>,
    ) -> Result<Self::Response, WireResponseError> {
        Err(WireResponseError::UnexpectedResponse)
    }

    fn into_wire(self, header: PartitionProcessorRpcRequestHeader) -> Self::Wire;

    fn from_wire(
        _request_id: PartitionProcessorRpcRequestId,
        response: <Self::Wire as RpcRequest>::Response,
    ) -> Result<Self::Response, WireResponseError> {
        Self::Wire::unwrap_response(response)?
            .try_into()
            .map_err(Into::into)
    }
}

/// Sends typed requests to the partition processor owning the request's partition key.
pub trait PartitionProcessorClient: Send + Sync {
    fn send<R: PartitionProcessorRpc>(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        request: R,
    ) -> impl Future<Output = Result<R::Response, PartitionProcessorClientError>> + Send;
}
