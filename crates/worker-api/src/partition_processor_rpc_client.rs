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

use restate_core::ShutdownError;
use restate_core::network::ConnectError;
use restate_core::network::{NetworkSender, RpcReplyError, Swimlane};
use restate_core::network::{Networking, TransportConnect};
use restate_core::partitions::PartitionRouting;
use restate_types::NodeId;
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, PartitionProcessorRpcRequestId};
use restate_types::live::Live;
use restate_types::net::ProtocolVersion;
use restate_types::net::codec::EncodeError;
use restate_types::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcRequest, PartitionProcessorRpcRequestHeader,
};
use restate_types::partition_processor::client::{
    PartitionProcessorClient, PartitionProcessorClientError, PartitionProcessorRpc,
    WireResponseError,
};
use restate_types::partition_table::{FindPartition, PartitionTable, PartitionTableError};

use crate::metric_definitions::{
    INVOCATION_CLIENT_REQUESTS, STATUS_COMPLETED, STATUS_INTERNAL_ERROR, STATUS_OVERLOADED_ERROR,
    STATUS_PROTOCOL_ERROR, STATUS_ROUTING_ERROR, STATUS_SHUTDOWN, STATUS_UNAVAILABLE_ERROR,
    describe_metrics,
};

#[derive(Debug, thiserror::Error)]
pub enum PartitionProcessorRpcClientError {
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
    /// The partition processor replied with a variant the request never expects.
    UnexpectedResponse,
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
            RpcErrorKind::UnexpectedResponse => {
                f.write_str("internal error: unexpected response from partition processor")
            }
        }
    }
}

impl PartitionProcessorRpcClientError {
    /// Returns true when the operation can be retried assuming no state mutation could have occurred in the PartitionProcessor.
    pub fn is_safe_to_retry(&self) -> bool {
        match self {
            PartitionProcessorRpcClientError::UnknownPartition(_)
            | PartitionProcessorRpcClientError::UnknownNode(_) => {
                // These are pre-flight error that we can distinguish,
                // and for which we know for certain that no message was proposed yet to the log.
                true
            }
            PartitionProcessorRpcClientError::Rpc(rpc) => rpc.is_safe_to_retry(),
            _ => false,
        }
    }

    fn as_metric_label(&self) -> &'static str {
        match self {
            PartitionProcessorRpcClientError::UnknownPartition(_)
            | PartitionProcessorRpcClientError::UnknownNode(_) => STATUS_ROUTING_ERROR,
            PartitionProcessorRpcClientError::Shutdown(_) => STATUS_SHUTDOWN,
            PartitionProcessorRpcClientError::Rpc(err) => err.source.as_metric_label(),
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
            | RpcErrorKind::UnexpectedResponse
            | RpcErrorKind::Reply(RpcReplyError::Unknown(_) | RpcReplyError::MessageUnrecognized) => {
                STATUS_PROTOCOL_ERROR
            }
            RpcErrorKind::Processor(PartitionProcessorRpcError::Internal(_)) => {
                STATUS_INTERNAL_ERROR
            }
        }
    }
}

impl From<WireResponseError> for RpcErrorKind {
    fn from(err: WireResponseError) -> Self {
        match err {
            WireResponseError::Processor(err) => Self::Processor(err),
            WireResponseError::UnexpectedResponse => Self::UnexpectedResponse,
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
            )
            // The request may have been applied already.
            | RpcErrorKind::UnexpectedResponse => false,
        }
    }
}

impl From<PartitionProcessorRpcClientError> for PartitionProcessorClientError {
    fn from(value: PartitionProcessorRpcClientError) -> Self {
        let is_safe_to_retry = value.is_safe_to_retry();
        Self::new(value, is_safe_to_retry)
    }
}

pub struct PartitionProcessorRpcClient<C> {
    networking: Networking<C>,
    partition_table: Live<PartitionTable>,
    partition_routing: PartitionRouting,
    partition_id_labels: Arc<HashMap<PartitionId, Arc<str>>>,
}

impl<C: Clone> Clone for PartitionProcessorRpcClient<C> {
    fn clone(&self) -> Self {
        Self {
            networking: self.networking.clone(),
            partition_table: self.partition_table.clone(),
            partition_routing: self.partition_routing.clone(),
            partition_id_labels: self.partition_id_labels.clone(),
        }
    }
}

impl<C> PartitionProcessorRpcClient<C> {
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

impl<C> PartitionProcessorRpcClient<C>
where
    C: TransportConnect,
{
    async fn resolve_partition_id_and_send<R: PartitionProcessorRpc>(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        request: R,
    ) -> Result<R::Response, PartitionProcessorRpcClientError> {
        let partition_id = self
            .partition_table
            .pinned()
            .find_partition_id(request.partition_key());
        let partition_id_label: metrics::SharedString = match partition_id.as_ref() {
            Ok(partition_id) => match self.partition_id_labels.get(partition_id) {
                Some(label) => label.clone().into(),
                None => partition_id.to_string().into(),
            },
            Err(_) => "unknown".into(),
        };

        let res = match partition_id {
            Ok(partition_id) => {
                self.send_to_partition(request_id, partition_id, request)
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

    async fn send_to_partition<R: PartitionProcessorRpc>(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        partition_id: PartitionId,
        request: R,
    ) -> Result<R::Response, PartitionProcessorRpcClientError> {
        let node_id = NodeId::from(
            self.partition_routing
                .get_node_by_partition(partition_id)
                .ok_or(PartitionProcessorRpcClientError::UnknownNode(partition_id))?,
        );

        // find connection for this node
        let connection = self
            .networking
            .get_connection(node_id, Swimlane::IngressData)
            .await
            .map_err(|err| RpcError::from_err(partition_id, node_id, err))?;

        // We should use the dedicated message format if:
        // 1. Our peer understands the dedicated message format (i.e. on protocol V4+), and the flag
        //    that controls this is enabled.
        // 2. The RPC doesn't have a legacy wire implementation (aka new RPCs). In this case, it's
        //    on the caller to make sure that the peer understands that new message before sending
        //    it, otherwise it'll get an unexpected message error from the peer.
        let use_dedicated_message = !R::HAS_LEGACY_WIRE
            || (connection.protocol_version() >= ProtocolVersion::V4
                && Configuration::pinned()
                    .common
                    .experimental
                    .is_partition_processor_dedicated_messages_enabled());

        let permit = connection.reserve().await.ok_or_else(|| {
            RpcError::from_err(
                partition_id,
                node_id,
                RpcErrorKind::ConnectionClosedBeforeSend,
            )
        })?;
        let header = PartitionProcessorRpcRequestHeader::new(request_id);
        let res = if use_dedicated_message {
            let request = request.into_wire(header);
            let response = permit
                .send_rpc(request, Some(*partition_id as u64))
                .map_err(|err| RpcError::from_err(partition_id, node_id, err))?
                .await
                .map_err(|err| RpcError::from_err(partition_id, node_id, err))?;
            R::from_wire(header.request_id, response)
        } else {
            let request = request.into_legacy_wire().expect("FIX ME");
            let response = permit
                .send_rpc(
                    PartitionProcessorRpcRequest::with_header(header, partition_id, request),
                    Some(*partition_id as u64),
                )
                .map_err(|err| RpcError::from_err(partition_id, node_id, err))?
                .await
                .map_err(|err| RpcError::from_err(partition_id, node_id, err))?;
            R::from_legacy_response(header.request_id, response)
        };

        Ok(res.map_err(|err| RpcError::from_err(partition_id, node_id, err))?)
    }
}

impl<C> PartitionProcessorClient for PartitionProcessorRpcClient<C>
where
    C: TransportConnect,
{
    async fn send<R: PartitionProcessorRpc>(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        request: R,
    ) -> Result<R::Response, PartitionProcessorClientError> {
        Ok(self
            .resolve_partition_id_and_send(request_id, request)
            .await?)
    }
}
