// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use metrics::counter;
use tracing::trace;

use restate_core::ShutdownError;
use restate_core::network::ConnectError;
use restate_core::network::{NetworkSender, RpcReplyError, Swimlane};
use restate_core::network::{Networking, TransportConnect};
use restate_core::partitions::PartitionRouting;
use restate_types::NodeId;
use restate_types::identifiers::{
    EntryIndex, InvocationId, PartitionId, PartitionProcessorRpcRequestId, WithPartitionKey,
};
use restate_types::invocation::client::{
    AttachInvocationResponse, CancelInvocationResponse, GetInvocationOutputResponse,
    GetInvocationStatusResponse, InvocationClient, InvocationClientError, InvocationOutput,
    KillInvocationResponse, PatchDeploymentId, PauseInvocationResponse, PurgeInvocationResponse,
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
use restate_types::time::MillisSinceEpoch;

use crate::metric_definitions::{
    INVOCATION_CLIENT_REQUESTS, STATUS_COMPLETED, STATUS_INTERNAL_ERROR, STATUS_OVERLOADED_ERROR,
    STATUS_PROTOCOL_ERROR, STATUS_ROUTING_ERROR, STATUS_SHUTDOWN, STATUS_UNAVAILABLE_ERROR,
    describe_metrics,
};

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
    Connect(#[from] ConnectError),
    ConnectionClosedBeforeSend,
    Encode(#[from] EncodeError),
    Reply(#[from] RpcReplyError),
    Processor(#[from] PartitionProcessorRpcError),
}

// Note: Those are customer facing errors (e.g. in http invocation response errors), so try to keep
// stable as much as possible.
impl fmt::Display for RpcErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcErrorKind::Connect(err) => err.fmt(f),
            RpcErrorKind::ConnectionClosedBeforeSend => {
                f.write_str("failed sending request: Connection lost")
            }
            RpcErrorKind::Encode(err) => write!(f, "failed sending request: {err}"),
            RpcErrorKind::Reply(
                RpcReplyError::ServiceNotFound
                | RpcReplyError::SortCodeNotFound
                | RpcReplyError::ServiceStopped,
            )
            | RpcErrorKind::Processor(PartitionProcessorRpcError::NotLeader(_)) => {
                f.write_str("not leader")
            }
            RpcErrorKind::Reply(RpcReplyError::LoadShedding | RpcReplyError::ServiceNotReady) => {
                f.write_str("rejecting rpc because the partition is too busy")
            }
            RpcErrorKind::Reply(err) => write!(f, "internal error: {err}"),
            RpcErrorKind::Processor(PartitionProcessorRpcError::LostLeadership(_)) => {
                f.write_str("lost leadership")
            }
            RpcErrorKind::Processor(PartitionProcessorRpcError::Internal(msg)) => {
                write!(f, "internal error: {msg}")
            }
        }
    }
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

    fn as_metric_label(&self) -> &'static str {
        match self {
            PartitionProcessorInvocationClientError::UnknownPartition(_)
            | PartitionProcessorInvocationClientError::UnknownNode(_) => STATUS_ROUTING_ERROR,
            PartitionProcessorInvocationClientError::Shutdown(_) => STATUS_SHUTDOWN,
            PartitionProcessorInvocationClientError::Rpc(err) => err.source.as_metric_label(),
        }
    }
}

impl RpcErrorKind {
    fn as_metric_label(&self) -> &'static str {
        match self {
            RpcErrorKind::Connect(ConnectError::Shutdown(_)) => STATUS_SHUTDOWN,
            RpcErrorKind::Connect(ConnectError::Discovery(_))
            | RpcErrorKind::Reply(
                RpcReplyError::ServiceNotFound
                | RpcReplyError::ServiceStopped
                | RpcReplyError::SortCodeNotFound,
            )
            | RpcErrorKind::Processor(
                PartitionProcessorRpcError::NotLeader(_)
                | PartitionProcessorRpcError::LostLeadership(_),
            ) => STATUS_ROUTING_ERROR,
            RpcErrorKind::Reply(RpcReplyError::LoadShedding) => STATUS_OVERLOADED_ERROR,
            RpcErrorKind::Connect(
                ConnectError::Handshake(_)
                | ConnectError::Throttled(_)
                | ConnectError::Transport(_),
            )
            | RpcErrorKind::ConnectionClosedBeforeSend
            | RpcErrorKind::Reply(
                RpcReplyError::Dropped
                | RpcReplyError::ConnectionClosed(_)
                | RpcReplyError::ServiceNotReady,
            ) => STATUS_UNAVAILABLE_ERROR,
            RpcErrorKind::Encode(_)
            | RpcErrorKind::Reply(RpcReplyError::Unknown(_) | RpcReplyError::MessageUnrecognized) => {
                STATUS_PROTOCOL_ERROR
            }
            RpcErrorKind::Processor(PartitionProcessorRpcError::Internal(_)) => {
                STATUS_INTERNAL_ERROR
            }
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
        match &self.source {
            RpcErrorKind::Connect(_)
            | RpcErrorKind::ConnectionClosedBeforeSend
            | RpcErrorKind::Encode(_) => {
                // These are pre-flight error that we can distinguish,
                // and for which we know for certain that no message was proposed yet to the log.
                true
            }
            RpcErrorKind::Reply(err) => !err.maybe_processed(),
            RpcErrorKind::Processor(PartitionProcessorRpcError::NotLeader(_)) => true,
            RpcErrorKind::Processor(
                PartitionProcessorRpcError::LostLeadership(_)
                | PartitionProcessorRpcError::Internal(_),
            ) => false,
        }
    }
}

impl From<PartitionProcessorInvocationClientError> for InvocationClientError {
    fn from(value: PartitionProcessorInvocationClientError) -> Self {
        let is_safe_to_retry = value.is_safe_to_retry();
        Self::new(value, is_safe_to_retry)
    }
}

pub struct PartitionProcessorInvocationClient<C> {
    networking: Networking<C>,
    partition_table: Live<PartitionTable>,
    partition_routing: PartitionRouting,
    partition_id_labels: Arc<HashMap<PartitionId, Arc<str>>>,
}

impl<C: Clone> Clone for PartitionProcessorInvocationClient<C> {
    fn clone(&self) -> Self {
        Self {
            networking: self.networking.clone(),
            partition_table: self.partition_table.clone(),
            partition_routing: self.partition_routing.clone(),
            partition_id_labels: self.partition_id_labels.clone(),
        }
    }
}

impl<C> PartitionProcessorInvocationClient<C> {
    pub fn new(
        networking: Networking<C>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
    ) -> Self {
        describe_metrics();
        let partition_id_labels = partition_table
            .pinned()
            .iter_ids()
            .map(|partition_id| (*partition_id, Arc::<str>::from(partition_id.to_string())))
            .collect();

        Self {
            networking,
            partition_table,
            partition_routing,
            partition_id_labels: Arc::new(partition_id_labels),
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
            .find_partition_id(inner_request.partition_key());
        let partition_id_label: metrics::SharedString = match partition_id.as_ref() {
            Ok(partition_id) => match self.partition_id_labels.get(partition_id) {
                Some(label) => label.clone().into(),
                None => partition_id.to_string().into(),
            },
            Err(_) => "unknown".into(),
        };

        let res = match partition_id {
            Ok(partition_id) => {
                self.send_to_partition(request_id, partition_id, inner_request)
                    .await
            }
            Err(err) => Err(err.into()),
        };

        counter!(
            INVOCATION_CLIENT_REQUESTS,
            "partition_id" => partition_id_label,
            "status" => match &res {
                Ok(_) => STATUS_COMPLETED,
                Err(err) => err.as_metric_label(),
            },
        )
        .increment(1);

        res
    }

    async fn send_to_partition(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
        inner_request: PartitionProcessorRpcRequestInner,
    ) -> Result<PartitionProcessorRpcResponse, PartitionProcessorInvocationClientError> {
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
                RpcErrorKind::ConnectionClosedBeforeSend,
            )
        })?;
        let rpc_result = permit
            .send_rpc(
                PartitionProcessorRpcRequest {
                    request_id,
                    partition_id,
                    sent_at: Some(MillisSinceEpoch::now()),
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

        let PartitionProcessorRpcResponse::Submitted(submit_notification) = response else {
            panic!("Expecting PartitionProcessorRpcResponse::Submitted");
        };
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

        let PartitionProcessorRpcResponse::Output(invocation_output) = response else {
            panic!("Expecting PartitionProcessorRpcResponse::Output");
        };
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

    async fn get_invocation_status(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    ) -> Result<GetInvocationStatusResponse, InvocationClientError> {
        let response = self
            .resolve_partition_id_and_send(
                request_id,
                PartitionProcessorRpcRequestInner::GetInvocationStatus { invocation_id },
            )
            .await?;

        Ok(match response {
            PartitionProcessorRpcResponse::NotFound => GetInvocationStatusResponse::NotFound,
            PartitionProcessorRpcResponse::Status(output) => {
                GetInvocationStatusResponse::Status(output)
            }
            _ => {
                panic!(
                    "Expecting either PartitionProcessorRpcResponse::Status or PartitionProcessorRpcResponse::NotFound"
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

        let PartitionProcessorRpcResponse::Appended = response else {
            panic!("Expecting PartitionProcessorRpcResponse::Appended");
        };

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

        let PartitionProcessorRpcResponse::Appended = response else {
            panic!("Expecting PartitionProcessorRpcResponse::Appended");
        };

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
