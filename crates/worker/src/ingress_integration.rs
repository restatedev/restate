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
use restate_core::network::partition_processor_rpc_client::PartitionProcessorRpcClient;
use restate_core::network::Networking;
use restate_ingress_http::InvocationStorageReader;
use restate_types::invocation::InvocationQuery;
use restate_types::net::partition_processor_manager::GetOutputResult;

#[derive(Clone)]
pub struct InvocationStorageReaderImpl {
    partition_processor_rpc_client: PartitionProcessorRpcClient<Networking>,
}

impl InvocationStorageReaderImpl {
    pub fn new(partition_processor_rpc_client: PartitionProcessorRpcClient<Networking>) -> Self {
        Self {
            partition_processor_rpc_client,
        }
    }
}

impl InvocationStorageReader for InvocationStorageReaderImpl {
    async fn get_output(&self, query: InvocationQuery) -> Result<GetOutputResult, Error> {
        self.partition_processor_rpc_client
            .get_invocation_output(query)
            .await
            .map_err(Into::into)
    }
}
