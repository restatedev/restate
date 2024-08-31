// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use assert2::let_assert;

use restate_types::identifiers::WithPartitionKey;
use restate_types::invocation::InvocationQuery;
use restate_types::live::Live;
use restate_types::net::partition_processor_manager::{
    GetOutputResult, PartitionProcessorRpcRequest, PartitionProcessorRpcResponse,
};
use restate_types::partition_table::{FindPartition, PartitionTable, PartitionTableError};

use crate::network::rpc_router::{RpcError, RpcRouter};
use crate::network::{NetworkSender, Outgoing};
use crate::{my_node_id, ShutdownError};

#[derive(Debug, thiserror::Error)]
pub enum GetInvocationOutputError {
    #[error(transparent)]
    UnknownPartition(#[from] PartitionTableError),
    #[error("failed sending request")]
    SendFailed,
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("operation failed: {0}")]
    Internal(String),
}

#[derive(Clone)]
pub struct PartitionProcessorRpcClient<N> {
    rpc_router: RpcRouter<PartitionProcessorRpcRequest, N>,
    partition_table: Live<PartitionTable>,
}

impl<N> PartitionProcessorRpcClient<N> {
    pub fn new(
        rpc_router: RpcRouter<PartitionProcessorRpcRequest, N>,
        partition_table: Live<PartitionTable>,
    ) -> Self {
        Self {
            rpc_router,
            partition_table,
        }
    }
}

impl<N> PartitionProcessorRpcClient<N>
where
    N: NetworkSender,
{
    pub async fn get_invocation_output(
        &self,
        invocation_query: InvocationQuery,
    ) -> Result<GetOutputResult, GetInvocationOutputError> {
        let partition_id = self
            .partition_table
            .pinned()
            .find_partition_id(invocation_query.partition_key())?;

        // todo: Find node on which the leader for the given partition runs
        let node_id = my_node_id();
        let response = self
            .rpc_router
            .call(Outgoing::new(
                node_id,
                PartitionProcessorRpcRequest::get_output(partition_id, invocation_query.clone()),
            ))
            .await?;

        // todo: Handle retry conditions
        let response = response
            .into_body()
            .map_err(|err| GetInvocationOutputError::Internal(err.to_string()))?;
        let_assert!(
            PartitionProcessorRpcResponse::GetOutputResult(get_output_result) = response,
            "Expecting GetOutputResult"
        );

        Ok(get_output_result)
    }
}

impl<T> From<RpcError<T>> for GetInvocationOutputError {
    fn from(value: RpcError<T>) -> Self {
        match value {
            RpcError::SendError(_) => GetInvocationOutputError::SendFailed,
            RpcError::Shutdown(err) => GetInvocationOutputError::Shutdown(err),
        }
    }
}
