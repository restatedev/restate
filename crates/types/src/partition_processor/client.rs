// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::{PartitionId, PartitionProcessorRpcRequestId, WithPartitionKey};
use crate::net::RpcRequest;
use crate::net::partition_processor::{PartitionLeaderService, PartitionProcessorRpcError};

/// Error returned by a [`PartitionProcessorClient`].
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

/// A typed request to a partition processor.
pub trait PartitionProcessorRpc: WithPartitionKey + Send + 'static {
    /// The typed response the caller receives.
    type Response: Send;

    /// The message sent over the network.
    type Wire: RpcRequest<Service = PartitionLeaderService>;

    /// Convert the request onto the wire message.
    fn into_wire(
        self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
    ) -> Self::Wire;

    /// Convert the wire response into [`Self::Response`].
    fn from_wire(
        request_id: PartitionProcessorRpcRequestId,
        response: <Self::Wire as RpcRequest>::Response,
    ) -> Result<Self::Response, WireResponseError>;
}

/// Sends typed requests to the partition processor owning the request's partition key.
pub trait PartitionProcessorClient: Send + Sync {
    fn send<R: PartitionProcessorRpc>(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        request: R,
    ) -> impl Future<Output = Result<R::Response, PartitionProcessorClientError>> + Send;
}
