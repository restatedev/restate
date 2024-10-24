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
use restate_core::network::NetworkSender;
use restate_ingress_http::{RequestDispatcher, RequestDispatcherError};
use restate_types::identifiers::PartitionProcessorRpcRequestId;
use restate_types::invocation::{
    InvocationQuery, InvocationResponse, InvocationTargetType, ServiceInvocation,
    WorkflowHandlerType,
};
use restate_types::net::partition_processor::{InvocationOutput, SubmittedInvocationNotification};
use restate_types::retries::RetryPolicy;
use std::time::Duration;

#[derive(Clone)]
pub struct RpcRequestDispatcher<N> {
    partition_processor_rpc_client: PartitionProcessorRpcClient<N>,
    retry_policy: RetryPolicy,
    rpc_timeout: Duration,
}

impl<N> RpcRequestDispatcher<N> {
    pub fn new(partition_processor_rpc_client: PartitionProcessorRpcClient<N>) -> Self {
        Self {
            partition_processor_rpc_client,
            // Totally random chosen!!!
            retry_policy: RetryPolicy::fixed_delay(Duration::from_millis(50), None),
            rpc_timeout: Duration::from_secs(60),
        }
    }
}

impl<N> RequestDispatcher for RpcRequestDispatcher<N>
where
    N: NetworkSender + 'static,
{
    async fn append_invocation_and_wait_submit_notification_if_needed(
        &self,
        service_invocation: ServiceInvocation,
    ) -> Result<SubmittedInvocationNotification, RequestDispatcherError> {
        let request_id = PartitionProcessorRpcRequestId::default();
        if service_invocation.idempotency_key.is_some()
            || service_invocation.invocation_target.invocation_target_ty()
                == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            // In this case we need to wait for the submit notification from the PP, this is safe to retry
            self.retry_policy
                .clone()
                .retry(|| async {
                    Ok(tokio::time::timeout(
                        self.rpc_timeout,
                        self.partition_processor_rpc_client
                            .append_invocation_and_wait_submit_notification(
                                request_id,
                                service_invocation.clone(),
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
                    .append_invocation(request_id, service_invocation),
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

    async fn append_invocation_and_wait_output(
        &self,
        service_invocation: ServiceInvocation,
    ) -> Result<InvocationOutput, RequestDispatcherError> {
        let request_id = PartitionProcessorRpcRequestId::default();
        if service_invocation.idempotency_key.is_some()
            || service_invocation.invocation_target.invocation_target_ty()
                == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            // For workflow or idempotent calls, it is safe to retry always
            self.retry_policy
                .clone()
                .retry(|| async {
                    Ok(tokio::time::timeout(
                        self.rpc_timeout,
                        self.partition_processor_rpc_client
                            .append_invocation_and_wait_output(
                                request_id,
                                service_invocation.clone(),
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
                    .append_invocation_and_wait_output(request_id, service_invocation),
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

    async fn append_invocation_response(
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
