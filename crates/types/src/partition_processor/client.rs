// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::PartitionProcessorRpcRequestId;
use crate::net::partition_processor::{
    PartitionProcessorRpcRequestInner, PartitionProcessorRpcResponse,
};

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

/// A typed request to a partition processor.
pub trait PartitionProcessorRpc: Send + 'static {
    /// The typed response the caller receives.
    type Response: Send;

    /// Lower the request onto the wire enum. Routing is derived from the wire enum's partition key,
    /// so nothing else is needed.
    fn into_inner(self) -> PartitionProcessorRpcRequestInner;

    /// Lift the wire response into [`Self::Response`].
    ///
    /// `None` means the partition processor replied with a variant this request never expects.
    /// The client turns that into an error, never a panic.
    fn from_response(
        request_id: PartitionProcessorRpcRequestId,
        response: PartitionProcessorRpcResponse,
    ) -> Option<Self::Response>;
}

/// Sends typed requests to the partition processor owning the request's partition key.
pub trait PartitionProcessorClient: Send + Sync {
    fn send<R: PartitionProcessorRpc>(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        request: R,
    ) -> impl Future<Output = Result<R::Response, PartitionProcessorClientError>> + Send;
}
