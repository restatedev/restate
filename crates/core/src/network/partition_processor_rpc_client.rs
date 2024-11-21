// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use assert2::let_assert;
use tracing::trace;

use restate_types::identifiers::{PartitionId, PartitionProcessorRpcRequestId, WithPartitionKey};
use restate_types::invocation::{InvocationQuery, InvocationRequest, InvocationResponse};
use restate_types::live::Live;
use restate_types::net::partition_processor::{
    AppendInvocationReplyOn, GetInvocationOutputResponseMode, InvocationOutput,
    PartitionProcessorRpcError, PartitionProcessorRpcRequest, PartitionProcessorRpcRequestInner,
    PartitionProcessorRpcResponse, SubmittedInvocationNotification,
};
use restate_types::partition_table::{FindPartition, PartitionTable, PartitionTableError};

use crate::network::rpc_router::{ConnectionAwareRpcError, ConnectionAwareRpcRouter, RpcError};
use crate::network::{HasConnection, Networking, Outgoing, TransportConnect};
use crate::partitions::PartitionRouting;
use crate::ShutdownError;

#[derive(Debug, thiserror::Error)]
pub enum PartitionProcessorRpcClientError {
    #[error(transparent)]
    UnknownPartition(#[from] PartitionTableError),
    #[error("cannot find node for partition {0}")]
    UnknownNode(PartitionId),
    #[error("failed sending request")]
    SendFailed,
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    ConnectionAwareRpcError(
        #[from] ConnectionAwareRpcError<Outgoing<PartitionProcessorRpcRequest, HasConnection>>,
    ),
    #[error("message has been routed to a node which is not leader for partition '{0}'")]
    NotLeader(PartitionId),
    #[error("message has been routed to a node which lost leadership for partition '{0}'")]
    LostLeadership(PartitionId),
    #[error("rejecting rpc because the partition is too busy")]
    Busy,
    #[error("internal error: {0}")]
    Internal(String),
    #[error("partition processor starting")]
    Starting,
    #[error("partition processor stopping")]
    Stopping,
}

impl PartitionProcessorRpcClientError {
    /// Returns true when the operation can be retried assuming no state mutation could have occurred in the PartitionProcessor.
    pub fn is_safe_to_retry(&self) -> bool {
        match self {
            PartitionProcessorRpcClientError::ConnectionAwareRpcError(
                ConnectionAwareRpcError::CannotEstablishConnectionToPeer { .. },
            )
            | PartitionProcessorRpcClientError::ConnectionAwareRpcError(
                ConnectionAwareRpcError::SendError { .. },
            )
            | PartitionProcessorRpcClientError::UnknownPartition(_)
            | PartitionProcessorRpcClientError::UnknownNode(_)
            | PartitionProcessorRpcClientError::NotLeader(_)
            | PartitionProcessorRpcClientError::Starting
            | PartitionProcessorRpcClientError::Stopping => {
                // These are pre-flight error that we can distinguish,
                // and for which we know for certain that no message was proposed yet to the log.
                true
            }
            _ => false,
        }
    }
}

impl From<PartitionProcessorRpcError> for PartitionProcessorRpcClientError {
    fn from(value: PartitionProcessorRpcError) -> Self {
        match value {
            PartitionProcessorRpcError::NotLeader(partition_id) => {
                PartitionProcessorRpcClientError::NotLeader(partition_id)
            }
            PartitionProcessorRpcError::LostLeadership(partition_id) => {
                PartitionProcessorRpcClientError::LostLeadership(partition_id)
            }
            PartitionProcessorRpcError::Busy => PartitionProcessorRpcClientError::Busy,
            PartitionProcessorRpcError::Internal(msg) => {
                PartitionProcessorRpcClientError::Internal(msg)
            }
            PartitionProcessorRpcError::Starting => PartitionProcessorRpcClientError::Starting,
            PartitionProcessorRpcError::Stopping => PartitionProcessorRpcClientError::Stopping,
        }
    }
}

impl<T> From<RpcError<T>> for PartitionProcessorRpcClientError {
    fn from(value: RpcError<T>) -> Self {
        match value {
            RpcError::SendError(_) => PartitionProcessorRpcClientError::SendFailed,
            RpcError::Shutdown(err) => PartitionProcessorRpcClientError::Shutdown(err),
        }
    }
}

#[derive(Debug, Clone)]
pub enum AttachInvocationResponse {
    NotFound,
    /// Returned when the invocation hasn't an idempotency key, nor it's a workflow run.
    NotSupported,
    Ready(InvocationOutput),
}

#[derive(Debug, Clone)]
pub enum GetInvocationOutputResponse {
    NotFound,
    /// The invocation was found, but it's still processing and a result is not ready yet.
    NotReady,
    /// Returned when the invocation hasn't an idempotency key, nor it's a workflow run.
    NotSupported,
    Ready(InvocationOutput),
}

pub struct PartitionProcessorRpcClient<C> {
    networking: Networking<C>,
    rpc_router: ConnectionAwareRpcRouter<PartitionProcessorRpcRequest>,
    partition_table: Live<PartitionTable>,
    partition_routing: PartitionRouting,
}

impl<C> Clone for PartitionProcessorRpcClient<C> {
    fn clone(&self) -> Self {
        Self {
            networking: self.networking.clone(),
            rpc_router: self.rpc_router.clone(),
            partition_table: self.partition_table.clone(),
            partition_routing: self.partition_routing.clone(),
        }
    }
}

impl<C> PartitionProcessorRpcClient<C> {
    pub fn new(
        networking: Networking<C>,
        rpc_router: ConnectionAwareRpcRouter<PartitionProcessorRpcRequest>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
    ) -> Self {
        Self {
            networking,
            rpc_router,
            partition_table,
            partition_routing,
        }
    }
}

impl<C> PartitionProcessorRpcClient<C>
where
    C: TransportConnect,
{
    /// Append the invocation to the log, returning as soon as the invocation was successfully appended.
    pub async fn append_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: InvocationRequest,
    ) -> Result<(), PartitionProcessorRpcClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::AppendInvocation(
                    invocation_request,
                    AppendInvocationReplyOn::Appended,
                ),
            )
            .await?;

        let_assert!(
            PartitionProcessorRpcResponse::Appended = response,
            "Expecting PartitionProcessorRpcResponse::Appended"
        );

        Ok(())
    }

    /// Append the invocation to the log, waiting for the submit notification emitted by the PartitionProcessor.
    pub async fn append_invocation_and_wait_submit_notification(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: InvocationRequest,
    ) -> Result<SubmittedInvocationNotification, PartitionProcessorRpcClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::AppendInvocation(
                    invocation_request,
                    AppendInvocationReplyOn::Submitted,
                ),
            )
            .await?;

        let_assert!(
            PartitionProcessorRpcResponse::Submitted(submit_notification) = response,
            "Expecting PartitionProcessorRpcResponse::Submitted"
        );
        debug_assert_eq!(
            request_id, submit_notification.request_id,
            "Conflicting submit notification received"
        );

        Ok(submit_notification)
    }

    /// Append the invocation and wait for its output.
    pub async fn append_invocation_and_wait_output(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: InvocationRequest,
    ) -> Result<InvocationOutput, PartitionProcessorRpcClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::AppendInvocation(
                    invocation_request,
                    AppendInvocationReplyOn::Output,
                ),
            )
            .await?;

        let_assert!(
            PartitionProcessorRpcResponse::Output(invocation_output) = response,
            "Expecting PartitionProcessorRpcResponse::Output"
        );
        debug_assert_eq!(
            request_id, invocation_output.request_id,
            "Conflicting invocation output received"
        );

        Ok(invocation_output)
    }

    pub async fn attach_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> Result<AttachInvocationResponse, PartitionProcessorRpcClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::GetInvocationOutput(
                    invocation_query,
                    GetInvocationOutputResponseMode::BlockWhenNotReady,
                ),
            )
            .await?;

        Ok(match response {
            PartitionProcessorRpcResponse::NotFound => AttachInvocationResponse::NotFound,
            PartitionProcessorRpcResponse::NotSupported => AttachInvocationResponse::NotSupported,
            PartitionProcessorRpcResponse::Output(output) => {
                AttachInvocationResponse::Ready(output)
            }
            _ => {
                panic!("Expecting either PartitionProcessorRpcResponse::Output or PartitionProcessorRpcResponse::NotFound or PartitionProcessorRpcResponse::NotSupported")
            }
        })
    }

    pub async fn get_invocation_output(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> Result<GetInvocationOutputResponse, PartitionProcessorRpcClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::GetInvocationOutput(
                    invocation_query,
                    GetInvocationOutputResponseMode::ReplyIfNotReady,
                ),
            )
            .await?;

        Ok(match response {
            PartitionProcessorRpcResponse::NotFound => GetInvocationOutputResponse::NotFound,
            PartitionProcessorRpcResponse::NotSupported => {
                GetInvocationOutputResponse::NotSupported
            }
            PartitionProcessorRpcResponse::NotReady => GetInvocationOutputResponse::NotReady,
            PartitionProcessorRpcResponse::Output(output) => {
                GetInvocationOutputResponse::Ready(output)
            }
            _ => {
                panic!("Expecting either PartitionProcessorRpcResponse::Output or PartitionProcessorRpcResponse::NotFound or PartitionProcessorRpcResponse::NotSupported or PartitionProcessorRpcResponse::NotReady")
            }
        })
    }

    pub async fn append_invocation_response(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_response: InvocationResponse,
    ) -> Result<(), PartitionProcessorRpcClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::AppendInvocationResponse(invocation_response),
            )
            .await?;

        let_assert!(
            PartitionProcessorRpcResponse::Appended = response,
            "Expecting PartitionProcessorRpcResponse::Appended"
        );

        Ok(())
    }

    async fn resolve_partition_id_and_send(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        inner_request: PartitionProcessorRpcRequestInner,
    ) -> Result<PartitionProcessorRpcResponse, PartitionProcessorRpcClientError> {
        let partition_id = self
            .partition_table
            .pinned()
            .find_partition_id(inner_request.partition_key())?;

        let node_id = self
            .partition_routing
            .get_node_by_partition(partition_id)
            .ok_or(PartitionProcessorRpcClientError::UnknownNode(partition_id))?;

        let rpc_result = self
            .rpc_router
            .call(
                &self.networking,
                node_id,
                PartitionProcessorRpcRequest {
                    request_id,
                    partition_id,
                    inner: inner_request,
                },
            )
            .await?
            .into_body();

        if rpc_result.is_err() && rpc_result.as_ref().unwrap_err().likely_stale_route() {
            trace!(
                ?partition_id,
                ?node_id,
                "Received Partition Processor error indicating possible stale route"
            );
            self.partition_routing.request_refresh();
        }

        Ok(rpc_result?)
    }
}
