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

use restate_types::identifiers::{
    InvocationId, PartitionId, PartitionProcessorRpcRequestId, WithPartitionKey,
};
use restate_types::invocation::{InvocationQuery, InvocationRequest, InvocationResponse};
use restate_types::journal_v2::Signal;
use restate_types::live::Live;
use restate_types::net::partition_processor::{
    AppendInvocationReplyOn, GetInvocationOutputResponseMode, InvocationOutput,
    PartitionProcessorRpcError, PartitionProcessorRpcRequest, PartitionProcessorRpcRequestInner,
    PartitionProcessorRpcResponse, SubmittedInvocationNotification,
};
use restate_types::partition_table::{FindPartition, PartitionTable, PartitionTableError};

use crate::ShutdownError;
use crate::network::{Networking, TransportConnect};
use crate::partitions::PartitionRouting;

use super::{ConnectError, NetworkSender, RpcReplyError, Swimlane};

#[derive(Debug, thiserror::Error)]
pub enum PartitionProcessorRpcClientError {
    #[error(transparent)]
    UnknownPartition(#[from] PartitionTableError),
    #[error("cannot find node for partition {0}")]
    UnknownNode(PartitionId),
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error("failed sending request")]
    SendFailed,
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("message has been routed to a node which is not the leader of the partition")]
    NotLeader,
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
            PartitionProcessorRpcClientError::UnknownPartition(_)
            | PartitionProcessorRpcClientError::Connect(_)
            | PartitionProcessorRpcClientError::UnknownNode(_)
            | PartitionProcessorRpcClientError::NotLeader
            | PartitionProcessorRpcClientError::Starting
            | PartitionProcessorRpcClientError::Busy
            | PartitionProcessorRpcClientError::Stopping => {
                // These are pre-flight error that we can distinguish,
                // and for which we know for certain that no message was proposed yet to the log.
                true
            }
            _ => false,
        }
    }
}

impl From<RpcReplyError> for PartitionProcessorRpcClientError {
    fn from(value: RpcReplyError) -> Self {
        match value {
            e @ RpcReplyError::Unknown(_) => Self::Internal(e.to_string()),
            // possibly Stopping is a better fit here
            e @ RpcReplyError::Dropped => Self::Internal(e.to_string()),
            // todo: perhaps this should be an explicit error
            e @ RpcReplyError::ConnectionClosed(_) => Self::Internal(e.to_string()),
            e @ RpcReplyError::MessageUnrecognized => Self::Internal(e.to_string()),
            // todo: perhaps consider this as Stopping? Consult with @till
            // This is likely a node that's not running the `worker` role
            e @ RpcReplyError::ServiceNotFound => Self::Internal(e.to_string()),
            RpcReplyError::SortCodeNotFound => Self::NotLeader,
            RpcReplyError::LoadShedding => Self::Busy,
            RpcReplyError::ServiceNotReady => Self::Busy,
            RpcReplyError::ServiceStopped => Self::Stopping,
            RpcReplyError::NotSent => Self::SendFailed,
        }
    }
}

impl From<PartitionProcessorRpcError> for PartitionProcessorRpcClientError {
    fn from(value: PartitionProcessorRpcError) -> Self {
        match value {
            PartitionProcessorRpcError::NotLeader(_) => PartitionProcessorRpcClientError::NotLeader,
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
    partition_table: Live<PartitionTable>,
    partition_routing: PartitionRouting,
}

impl<C: Clone> Clone for PartitionProcessorRpcClient<C> {
    fn clone(&self) -> Self {
        Self {
            networking: self.networking.clone(),
            partition_table: self.partition_table.clone(),
            partition_routing: self.partition_routing.clone(),
        }
    }
}

impl<C> PartitionProcessorRpcClient<C> {
    pub fn new(
        networking: Networking<C>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
    ) -> Self {
        Self {
            networking,
            partition_table,
            partition_routing,
        }
    }
}

impl<C> PartitionProcessorRpcClient<C>
where
    C: TransportConnect,
{
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
                panic!(
                    "Expecting either PartitionProcessorRpcResponse::Output or PartitionProcessorRpcResponse::NotFound or PartitionProcessorRpcResponse::NotSupported"
                )
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
                panic!(
                    "Expecting either PartitionProcessorRpcResponse::Output or PartitionProcessorRpcResponse::NotFound or PartitionProcessorRpcResponse::NotSupported or PartitionProcessorRpcResponse::NotReady"
                )
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

    pub async fn append_signal(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
        signal: Signal,
    ) -> Result<(), PartitionProcessorRpcClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::AppendSignal(invocation_id, signal),
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

        // find connection for this node
        let connection = self
            .networking
            .get_connection(node_id, Swimlane::IngressData)
            .await?;
        let permit = connection
            .reserve()
            .await
            .ok_or(PartitionProcessorRpcClientError::SendFailed)?;
        let rpc_result = permit
            .send_rpc(
                PartitionProcessorRpcRequest {
                    request_id,
                    partition_id,
                    inner: inner_request,
                },
                Some(*partition_id as u64),
            )
            .await?;

        if rpc_result.is_err() && rpc_result.as_ref().unwrap_err().likely_stale_route() {
            trace!(
                %partition_id,
                %node_id,
                %request_id,
                "Received Partition Processor error indicating possible stale route"
            );
        }

        Ok(rpc_result?)
    }
}
