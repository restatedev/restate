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

#[derive(Clone)]
pub struct RpcRequestDispatcher<N> {
    partition_processor_rpc_client: PartitionProcessorRpcClient<N>,
}

impl<N> RpcRequestDispatcher<N> {
    pub fn new(partition_processor_rpc_client: PartitionProcessorRpcClient<N>) -> Self {
        Self {
            partition_processor_rpc_client,
        }
    }
}

impl<N> RequestDispatcher for RpcRequestDispatcher<N>
where
    N: NetworkSender + 'static,
{
    async fn append_invocation(
        &self,
        service_invocation: ServiceInvocation,
    ) -> Result<(), RequestDispatcherError> {
        // TODO figure out retry strategy
        self.partition_processor_rpc_client
            .append_invocation(
                PartitionProcessorRpcRequestId::default(),
                service_invocation,
            )
            .await
            .context("error when trying to interact with partition processor")?;
        Ok(())
    }

    async fn submit_invocation_and_wait_submit_notification_if_needed(
        &self,
        service_invocation: ServiceInvocation,
    ) -> Result<SubmittedInvocationNotification, RequestDispatcherError> {
        // TODO figure out retry strategy
        let request_id = PartitionProcessorRpcRequestId::default();
        if service_invocation.idempotency_key.is_some()
            || service_invocation.invocation_target.invocation_target_ty()
                == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            // In this case we need to wait for the submit notification from the PP
            Ok(self
                .partition_processor_rpc_client
                .append_invocation_and_wait_submit_notification(request_id, service_invocation)
                .await
                .context("error when trying to interact with partition processor")?)
        } else {
            // In this case we just need to wait the invocation was appended
            self.partition_processor_rpc_client
                .append_invocation(request_id, service_invocation)
                .await
                .context("error when trying to interact with partition processor")?;
            Ok(SubmittedInvocationNotification {
                request_id,
                is_new_invocation: true,
            })
        }
    }

    async fn submit_invocation_and_wait_output(
        &self,
        service_invocation: ServiceInvocation,
    ) -> Result<InvocationOutput, RequestDispatcherError> {
        // TODO figure out retry strategy
        Ok(self
            .partition_processor_rpc_client
            .append_invocation_and_wait_output(
                PartitionProcessorRpcRequestId::default(),
                service_invocation,
            )
            .await
            .context("error when trying to interact with partition processor")?)
    }

    async fn attach_invocation(
        &self,
        invocation_query: InvocationQuery,
    ) -> Result<AttachInvocationResponse, RequestDispatcherError> {
        // TODO figure out retry strategy
        Ok(self
            .partition_processor_rpc_client
            .attach_invocation(PartitionProcessorRpcRequestId::default(), invocation_query)
            .await
            .context("error when trying to interact with partition processor")?)
    }

    async fn get_invocation_output(
        &self,
        invocation_query: InvocationQuery,
    ) -> Result<GetInvocationOutputResponse, RequestDispatcherError> {
        Ok(self
            .partition_processor_rpc_client
            .get_invocation_output(PartitionProcessorRpcRequestId::default(), invocation_query)
            .await
            .context("error when trying to interact with partition processor")?)
    }

    async fn submit_invocation_response(
        &self,
        invocation_response: InvocationResponse,
    ) -> Result<(), RequestDispatcherError> {
        // TODO figure out retry strategy
        Ok(self
            .partition_processor_rpc_client
            .append_invocation_response(
                PartitionProcessorRpcRequestId::default(),
                invocation_response,
            )
            .await
            .context("error when trying to interact with partition processor")?)
    }
}
