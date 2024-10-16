// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Error;
use restate_core::network::partition_processor_rpc_client::GetInvocationOutputResponse;
use restate_core::network::partition_processor_rpc_client::PartitionProcessorRpcClient;
use restate_core::network::NetworkSender;
use restate_ingress_http::InvocationStorageReader;
use restate_types::identifiers::PartitionProcessorRpcRequestId;
use restate_types::invocation::InvocationQuery;

#[derive(Clone)]
pub struct InvocationStorageReaderImpl<N> {
    partition_processor_rpc_client: PartitionProcessorRpcClient<N>,
}

impl<N> InvocationStorageReaderImpl<N> {
    pub fn new(partition_processor_rpc_client: PartitionProcessorRpcClient<N>) -> Self {
        Self {
            partition_processor_rpc_client,
        }
    }
}

impl<N> InvocationStorageReader for InvocationStorageReaderImpl<N>
where
    N: NetworkSender + 'static,
{
    async fn get_output(
        &self,
        query: InvocationQuery,
    ) -> Result<GetInvocationOutputResponse, Error> {
        self.partition_processor_rpc_client
            .get_invocation_output(PartitionProcessorRpcRequestId::default(), query)
            .await
            .map_err(Into::into)
    }
}
