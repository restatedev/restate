// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{RequestDispatcher, RequestDispatcherError};
use anyhow::anyhow;
use restate_core::network::partition_processor_rpc_client::{
    AttachInvocationResponse, GetInvocationOutputResponse,
};
use restate_core::network::partition_processor_rpc_client::{
    PartitionProcessorRpcClient, PartitionProcessorRpcClientError,
};
use restate_core::network::TransportConnect;
use restate_types::identifiers::{PartitionProcessorRpcRequestId, WithInvocationId};
use restate_types::invocation::{InvocationQuery, InvocationRequest, InvocationResponse};
use restate_types::net::partition_processor::{InvocationOutput, SubmittedInvocationNotification};
use restate_types::retries::RetryPolicy;
use std::future::Future;
use std::time::Duration;
use tracing::{debug_span, trace, Instrument};

pub struct RpcRequestDispatcher<C> {
    partition_processor_rpc_client: PartitionProcessorRpcClient<C>,
    retry_policy: RetryPolicy,
}

impl<T> Clone for RpcRequestDispatcher<T> {
    fn clone(&self) -> Self {
        RpcRequestDispatcher {
            partition_processor_rpc_client: self.partition_processor_rpc_client.clone(),
            retry_policy: self.retry_policy.clone(),
        }
    }
}

impl<C> RpcRequestDispatcher<C> {
    pub fn new(partition_processor_rpc_client: PartitionProcessorRpcClient<C>) -> Self {
        Self {
            partition_processor_rpc_client,
            // TODO figure out how to tune this?
            retry_policy: RetryPolicy::fixed_delay(Duration::from_millis(50), None),
        }
    }

    async fn execute_rpc<Fn, Fut, T>(
        &self,
        is_idempotent: bool,
        operation: Fn,
    ) -> Result<T, RequestDispatcherError>
    where
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<T, PartitionProcessorRpcClientError>>,
    {
        Ok(self
            .retry_policy
            .clone()
            .retry_if(operation, |e| {
                let retry = is_idempotent || e.is_safe_to_retry();

                if retry {
                    trace!("Retrying rpc because of error: {e}.");
                } else {
                    trace!("Rpc failed: {e}");
                }

                retry
            })
            .await
            .map_err(|e| anyhow!("Error when trying to route the request internally: {e}"))?)
    }
}

impl<C> RequestDispatcher for RpcRequestDispatcher<C>
where
    C: TransportConnect,
{
    async fn send(
        &self,
        invocation_request: InvocationRequest,
    ) -> Result<SubmittedInvocationNotification, RequestDispatcherError> {
        let request_id = PartitionProcessorRpcRequestId::default();
        let is_idempotent = invocation_request.is_idempotent();
        self.execute_rpc(is_idempotent, || {
            self.partition_processor_rpc_client
                .append_invocation_and_wait_submit_notification(
                    request_id,
                    invocation_request.clone(),
                )
        })
        .instrument(debug_span!("send invocation", %request_id, invocation_id = %invocation_request.invocation_id()))
        .await
    }

    async fn call(
        &self,
        invocation_request: InvocationRequest,
    ) -> Result<InvocationOutput, RequestDispatcherError> {
        let request_id = PartitionProcessorRpcRequestId::default();
        let is_idempotent = invocation_request.is_idempotent();
        self.execute_rpc(is_idempotent, || {
            self.partition_processor_rpc_client
                .append_invocation_and_wait_output(request_id, invocation_request.clone())
        })
        .instrument(debug_span!("call invocation", %request_id, invocation_id = %invocation_request.invocation_id()))
        .await
    }

    async fn attach_invocation(
        &self,
        invocation_query: InvocationQuery,
    ) -> Result<AttachInvocationResponse, RequestDispatcherError> {
        let request_id = PartitionProcessorRpcRequestId::default();
        self.execute_rpc(true, || {
            self.partition_processor_rpc_client
                .attach_invocation(request_id, invocation_query.clone())
        })
        .instrument(debug_span!("attach to invocation", %request_id, invocation_id = %invocation_query.to_invocation_id()))
        .await
    }

    async fn get_invocation_output(
        &self,
        invocation_query: InvocationQuery,
    ) -> Result<GetInvocationOutputResponse, RequestDispatcherError> {
        let request_id = PartitionProcessorRpcRequestId::default();
        self.execute_rpc(true, || {
            self.partition_processor_rpc_client
                .get_invocation_output(request_id, invocation_query.clone())
        })
        .instrument(debug_span!("get invocation output", %request_id, invocation_id = %invocation_query.to_invocation_id()))
        .await
    }

    async fn send_invocation_response(
        &self,
        invocation_response: InvocationResponse,
    ) -> Result<(), RequestDispatcherError> {
        let request_id = PartitionProcessorRpcRequestId::default();
        self.execute_rpc(true, || {
            self.partition_processor_rpc_client
                .append_invocation_response(request_id, invocation_response.clone())
        })
        .instrument(debug_span!("send invocation response", %request_id, invocation_id = %invocation_response.id))
        .await
    }
}
