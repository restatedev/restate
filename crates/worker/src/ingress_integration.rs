// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use restate_core::network::partition_processor_rpc_client::PartitionProcessorRpcClient;
use restate_core::network::partition_processor_rpc_client::{
    AttachInvocationResponse, GetInvocationOutputResponse,
};
use restate_core::network::TransportConnect;
use restate_ingress_http::{RequestDispatcher, RequestDispatcherError};
use restate_types::identifiers::PartitionProcessorRpcRequestId;
use restate_types::invocation::{InvocationQuery, InvocationRequest, InvocationResponse};
use restate_types::net::partition_processor::{InvocationOutput, SubmittedInvocationNotification};
use restate_types::retries::RetryPolicy;
use std::time::Duration;

pub struct RpcRequestDispatcher<C> {
    partition_processor_rpc_client: PartitionProcessorRpcClient<C>,
    retry_policy: RetryPolicy,
    rpc_timeout: Duration,
}

impl<T> Clone for RpcRequestDispatcher<T> {
    fn clone(&self) -> Self {
        RpcRequestDispatcher {
            partition_processor_rpc_client: self.partition_processor_rpc_client.clone(),
            retry_policy: self.retry_policy.clone(),
            rpc_timeout: self.rpc_timeout,
        }
    }
}

impl<C> RpcRequestDispatcher<C> {
    pub fn new(partition_processor_rpc_client: PartitionProcessorRpcClient<C>) -> Self {
        Self {
            partition_processor_rpc_client,
            // Totally random chosen!!!
            retry_policy: RetryPolicy::fixed_delay(Duration::from_millis(50), None),
            rpc_timeout: Duration::from_secs(60),
        }
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
        if invocation_request.is_idempotent() {
            // In this case we need to wait for the submit notification from the PP, this is safe to retry
            self.retry_policy
                .clone()
                .retry(|| async {
                    Ok(tokio::time::timeout(
                        self.rpc_timeout,
                        self.partition_processor_rpc_client
                            .append_invocation_and_wait_submit_notification(
                                request_id,
                                invocation_request.clone(),
                            ),
                    )
                    .await
                    .context("timeout while trying to reach partition processor")?
                    .context("error when trying to interact with partition processor")?)
                })
                .await
        } else {
            // In this case we just need to wait the invocation was appended
            tokio::time::timeout(
                self.rpc_timeout,
                self.partition_processor_rpc_client
                    .append_invocation(request_id, invocation_request),
            )
            .await
            .context("timeout while trying to reach partition processor")?
            .context("error when trying to interact with partition processor")?;
            Ok(SubmittedInvocationNotification {
                request_id,
                is_new_invocation: true,
            })
        }
    }

    async fn call(
        &self,
        invocation_request: InvocationRequest,
    ) -> Result<InvocationOutput, RequestDispatcherError> {
        let request_id = PartitionProcessorRpcRequestId::default();
        if invocation_request.is_idempotent() {
            // For workflow or idempotent calls, it is safe to retry always
            self.retry_policy
                .clone()
                .retry(|| async {
                    Ok(tokio::time::timeout(
                        self.rpc_timeout,
                        self.partition_processor_rpc_client
                            .append_invocation_and_wait_output(
                                request_id,
                                invocation_request.clone(),
                            ),
                    )
                    .await
                    .context("timeout while trying to reach partition processor")?
                    .context("error when trying to interact with partition processor")?)
                })
                .await
        } else {
            Ok(tokio::time::timeout(
                self.rpc_timeout,
                self.partition_processor_rpc_client
                    .append_invocation_and_wait_output(request_id, invocation_request),
            )
            .await
            .context("timeout while trying to reach partition processor")?
            .context("error when trying to interact with partition processor")?)
        }
    }

    async fn attach_invocation(
        &self,
        invocation_query: InvocationQuery,
    ) -> Result<AttachInvocationResponse, RequestDispatcherError> {
        let request_id = PartitionProcessorRpcRequestId::default();
        // Attaching an invocation is idempotent and can be retried, with timeouts
        self.retry_policy
            .clone()
            .retry(|| async {
                Ok(tokio::time::timeout(
                    self.rpc_timeout,
                    self.partition_processor_rpc_client
                        .attach_invocation(request_id, invocation_query.clone()),
                )
                .await
                .context("timeout while trying to reach partition processor")?
                .context("error when trying to interact with partition processor")?)
            })
            .await
    }

    async fn get_invocation_output(
        &self,
        invocation_query: InvocationQuery,
    ) -> Result<GetInvocationOutputResponse, RequestDispatcherError> {
        // No need to retry this
        Ok(tokio::time::timeout(
            self.rpc_timeout,
            self.partition_processor_rpc_client
                .get_invocation_output(PartitionProcessorRpcRequestId::default(), invocation_query),
        )
        .await
        .context("timeout while trying to reach partition processor")?
        .context("error when trying to interact with partition processor")?)
    }

    async fn send_invocation_response(
        &self,
        invocation_response: InvocationResponse,
    ) -> Result<(), RequestDispatcherError> {
        let request_id = PartitionProcessorRpcRequestId::default();
        // Appending invocation response is an idempotent operation, it's always safe to try
        self.retry_policy
            .clone()
            .retry(|| async {
                Ok(self
                    .partition_processor_rpc_client
                    .append_invocation_response(request_id, invocation_response.clone())
                    .await
                    .context("error when trying to interact with partition processor")?)
            })
            .await
    }
}
