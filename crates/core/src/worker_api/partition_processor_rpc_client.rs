// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ShutdownError;
use crate::network::ConnectError;
use crate::network::{NetworkSender, RpcReplyError, Swimlane};
use crate::network::{Networking, TransportConnect};
use crate::partitions::PartitionRouting;
use assert2::let_assert;
use restate_types::NodeId;
use restate_types::errors::GenericError;
use restate_types::identifiers::{
    EntryIndex, InvocationId, PartitionId, PartitionProcessorRpcRequestId, WithPartitionKey,
};
use restate_types::invocation::client::{
    AttachInvocationResponse, CancelInvocationResponse, GetInvocationOutputResponse,
    InvocationClient, InvocationClientError, InvocationOutput, KillInvocationResponse,
    PatchDeploymentId, PauseInvocationResponse, PurgeInvocationResponse,
    RestartAsNewInvocationResponse, ResumeInvocationResponse, SubmittedInvocationNotification,
};
use restate_types::invocation::{InvocationQuery, InvocationRequest, InvocationResponse};
use restate_types::journal_v2::Signal;
use restate_types::live::Live;
use restate_types::net::codec::EncodeError;
use restate_types::net::partition_processor::{
    AppendInvocationReplyOn, GetInvocationOutputResponseMode, PartitionProcessorRpcError,
    PartitionProcessorRpcRequest, PartitionProcessorRpcRequestInner, PartitionProcessorRpcResponse,
};
use restate_types::partition_table::{FindPartition, PartitionTable, PartitionTableError};
use std::sync::Arc;
use tracing::trace;

#[derive(Debug, thiserror::Error)]
pub enum PartitionProcessorInvocationClientError {
    #[error(transparent)]
    UnknownPartition(#[from] PartitionTableError),
    #[error("cannot find node for partition {0}")]
    UnknownNode(PartitionId),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Rpc(#[from] RpcError),
}

#[derive(Debug, thiserror::Error)]
#[error("rpc for partition {partition_id} at node {node_id} failed: {source}")]
pub struct RpcError {
    partition_id: PartitionId,
    node_id: NodeId,
    #[source]
    source: RpcErrorKind,
}

#[derive(Debug, thiserror::Error)]
pub enum RpcErrorKind {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error("failed sending request: {0}")]
    SendFailed(GenericError),
    #[error("not leader")]
    NotLeader,
    #[error("lost leadership")]
    LostLeadership,
    #[error("rejecting rpc because the partition is too busy")]
    Busy,
    #[error("internal error: {0}")]
    Internal(String),
}

impl PartitionProcessorInvocationClientError {
    /// Returns true when the operation can be retried assuming no state mutation could have occurred in the PartitionProcessor.
    pub fn is_safe_to_retry(&self) -> bool {
        match self {
            PartitionProcessorInvocationClientError::UnknownPartition(_)
            | PartitionProcessorInvocationClientError::UnknownNode(_) => {
                // These are pre-flight error that we can distinguish,
                // and for which we know for certain that no message was proposed yet to the log.
                true
            }
            PartitionProcessorInvocationClientError::Rpc(rpc) => rpc.is_safe_to_retry(),
            _ => false,
        }
    }
}

impl RpcError {
    fn from_err(partition_id: PartitionId, node_id: NodeId, err: impl Into<RpcErrorKind>) -> Self {
        Self {
            partition_id,
            node_id,
            source: err.into(),
        }
    }

    fn is_safe_to_retry(&self) -> bool {
        match self.source {
            RpcErrorKind::Connect(_)
            | RpcErrorKind::NotLeader
            | RpcErrorKind::Busy
            | RpcErrorKind::SendFailed(_) => {
                // These are pre-flight error that we can distinguish,
                // and for which we know for certain that no message was proposed yet to the log.
                true
            }
            _ => false,
        }
    }
}

impl From<PartitionProcessorInvocationClientError> for InvocationClientError {
    fn from(value: PartitionProcessorInvocationClientError) -> Self {
        let is_safe_to_retry = value.is_safe_to_retry();
        Self::new(value, is_safe_to_retry)
    }
}

impl From<EncodeError> for RpcErrorKind {
    fn from(value: EncodeError) -> Self {
        Self::SendFailed(value.into())
    }
}

impl From<RpcReplyError> for RpcErrorKind {
    fn from(value: RpcReplyError) -> Self {
        match value {
            e @ RpcReplyError::Unknown(_) => Self::Internal(e.to_string()),
            e @ RpcReplyError::Dropped => Self::Internal(e.to_string()),
            // todo: perhaps this should be an explicit error
            e @ RpcReplyError::ConnectionClosed(_) => Self::Internal(e.to_string()),
            e @ RpcReplyError::MessageUnrecognized => Self::Internal(e.to_string()),
            e @ RpcReplyError::InvalidPayload => Self::Internal(e.to_string()),
            RpcReplyError::ServiceNotFound | RpcReplyError::SortCodeNotFound => Self::NotLeader,
            RpcReplyError::LoadShedding => Self::Busy,
            RpcReplyError::ServiceNotReady => Self::Busy,
            RpcReplyError::ServiceStopped => Self::NotLeader,
        }
    }
}

impl From<PartitionProcessorRpcError> for RpcErrorKind {
    fn from(value: PartitionProcessorRpcError) -> Self {
        match value {
            PartitionProcessorRpcError::NotLeader(_) => RpcErrorKind::NotLeader,
            PartitionProcessorRpcError::LostLeadership(_) => RpcErrorKind::LostLeadership,
            PartitionProcessorRpcError::Internal(msg) => RpcErrorKind::Internal(msg),
        }
    }
}

pub struct PartitionProcessorInvocationClient<C> {
    networking: Networking<C>,
    partition_table: Live<PartitionTable>,
    partition_routing: PartitionRouting,
}

impl<C: Clone> Clone for PartitionProcessorInvocationClient<C> {
    fn clone(&self) -> Self {
        Self {
            networking: self.networking.clone(),
            partition_table: self.partition_table.clone(),
            partition_routing: self.partition_routing.clone(),
        }
    }
}

impl<C> PartitionProcessorInvocationClient<C> {
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

impl<C> PartitionProcessorInvocationClient<C>
where
    C: TransportConnect,
{
    async fn resolve_partition_id_and_send(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        inner_request: PartitionProcessorRpcRequestInner,
    ) -> Result<PartitionProcessorRpcResponse, PartitionProcessorInvocationClientError> {
        let partition_id = self
            .partition_table
            .pinned()
            .find_partition_id(inner_request.partition_key())?;

        let node_id = NodeId::from(
            self.partition_routing
                .get_node_by_partition(partition_id)
                .ok_or(PartitionProcessorInvocationClientError::UnknownNode(
                    partition_id,
                ))?,
        );

        // find connection for this node
        let connection = self
            .networking
            .get_connection(node_id, Swimlane::IngressData)
            .await
            .map_err(|err| RpcError::from_err(partition_id, node_id, err))?;

        let permit = connection.reserve().await.ok_or_else(|| {
            RpcError::from_err(
                partition_id,
                node_id,
                RpcErrorKind::SendFailed("Connection lost".into()),
            )
        })?;
        let rpc_result = permit
            .send_rpc(
                PartitionProcessorRpcRequest {
                    request_id,
                    partition_id,
                    inner: inner_request,
                },
                Some(*partition_id as u64),
            )
            .map_err(|err| RpcError::from_err(partition_id, node_id, err))?
            .await
            .map_err(|err| RpcError::from_err(partition_id, node_id, err))?;

        if rpc_result.is_err() && rpc_result.as_ref().unwrap_err().likely_stale_route() {
            trace!(
                %partition_id,
                %node_id,
                %request_id,
                "Received Partition Processor error indicating possible stale route"
            );
        }

        Ok(rpc_result.map_err(|err| RpcError::from_err(partition_id, node_id, err))?)
    }
}

impl<C> InvocationClient for PartitionProcessorInvocationClient<C>
where
    C: TransportConnect,
{
    /// Append the invocation to the log, waiting for the submit notification emitted by the PartitionProcessor.
    async fn append_invocation_and_wait_submit_notification(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: Arc<InvocationRequest>,
    ) -> Result<SubmittedInvocationNotification, InvocationClientError> {
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
    async fn append_invocation_and_wait_output(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: Arc<InvocationRequest>,
    ) -> Result<InvocationOutput, InvocationClientError> {
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
    async fn attach_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> Result<AttachInvocationResponse, InvocationClientError> {
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
    async fn get_invocation_output(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> Result<GetInvocationOutputResponse, InvocationClientError> {
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
    async fn append_invocation_response(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_response: InvocationResponse,
    ) -> Result<(), InvocationClientError> {
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
    async fn append_signal(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
        signal: Signal,
    ) -> Result<(), InvocationClientError> {
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

    async fn cancel_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> Result<CancelInvocationResponse, InvocationClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::CancelInvocation { invocation_id },
            )
            .await?;

        Ok(match response {
            PartitionProcessorRpcResponse::CancelInvocation(cancel_invocation_response) => {
                cancel_invocation_response.into()
            }
            _ => {
                panic!("Expecting CancelInvocation rpc response")
            }
        })
    }

    async fn kill_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> Result<KillInvocationResponse, InvocationClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::KillInvocation { invocation_id },
            )
            .await?;

        Ok(match response {
            PartitionProcessorRpcResponse::KillInvocation(kill_invocation_response) => {
                kill_invocation_response.into()
            }
            _ => {
                panic!("Expecting KillInvocation rpc response")
            }
        })
    }

    async fn purge_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> Result<PurgeInvocationResponse, InvocationClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::PurgeInvocation { invocation_id },
            )
            .await?;

        Ok(match response {
            PartitionProcessorRpcResponse::PurgeInvocation(purge_invocation_response) => {
                purge_invocation_response.into()
            }
            _ => {
                panic!("Expecting PurgeInvocation rpc response")
            }
        })
    }

    async fn purge_journal(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> Result<PurgeInvocationResponse, InvocationClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::PurgeJournal { invocation_id },
            )
            .await?;

        Ok(match response {
            PartitionProcessorRpcResponse::PurgeJournal(purge_invocation_response) => {
                purge_invocation_response.into()
            }
            _ => {
                panic!("Expecting PurgeInvocation rpc response")
            }
        })
    }

    async fn restart_as_new_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
        copy_prefix_up_to_index_included: EntryIndex,
        patch_deployment_id: PatchDeploymentId,
    ) -> Result<RestartAsNewInvocationResponse, InvocationClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::RestartAsNewInvocation {
                    invocation_id,
                    copy_prefix_up_to_index_included,
                    patch_deployment_id,
                },
            )
            .await?;

        Ok(match response {
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                restart_as_new_invocation_response,
            ) => restart_as_new_invocation_response.into(),
            _ => {
                panic!("Expecting RestartAsNewInvocation rpc response")
            }
        })
    }

    async fn resume_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
        deployment_id: PatchDeploymentId,
    ) -> Result<ResumeInvocationResponse, InvocationClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::ResumeInvocation {
                    invocation_id,
                    deployment_id,
                },
            )
            .await?;

        Ok(match response {
            PartitionProcessorRpcResponse::ResumeInvocation(resume_invocation_response) => {
                resume_invocation_response.into()
            }
            _ => {
                panic!("Expecting ResumeInvocation rpc response")
            }
        })
    }

    async fn pause_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> Result<PauseInvocationResponse, InvocationClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::PauseInvocation { invocation_id },
            )
            .await?;

        Ok(match response {
            PartitionProcessorRpcResponse::PauseInvocation(pause_invocation_response) => {
                pause_invocation_response.into()
            }
            _ => {
                panic!("Expecting PauseInvocation rpc response")
            }
        })
    }
}
