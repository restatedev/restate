// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::partition_processor_rpc_client::PartitionProcessorRpcClient;
use restate_core::network::rpc_router::ConnectionAwareRpcRouter;
use restate_core::network::{MessageRouterBuilder, Networking, TransportConnect};
use restate_core::partitions::PartitionRouting;
use restate_ingress_http::rpc_request_dispatcher::RpcRequestDispatcher;
use restate_ingress_http::HyperServerIngress;
use restate_types::config::IngressOptions;
use restate_types::health::HealthStatus;
use restate_types::live::{BoxedLiveLoad, Live};
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::common::IngressStatus;
use restate_types::schema::Schema;

type IngressHttp<T> = HyperServerIngress<Schema, RpcRequestDispatcher<T>>;

pub struct IngressRole<T> {
    ingress_http: IngressHttp<T>,
}

impl<T: TransportConnect> IngressRole<T> {
    pub fn create(
        mut ingress_options: BoxedLiveLoad<IngressOptions>,
        health: HealthStatus<IngressStatus>,
        networking: Networking<T>,
        schema: Live<Schema>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let rpc_router = ConnectionAwareRpcRouter::new(router_builder);

        let dispatcher = RpcRequestDispatcher::new(PartitionProcessorRpcClient::new(
            networking,
            rpc_router,
            partition_table,
            partition_routing,
        ));
        let ingress_http = HyperServerIngress::from_options(
            ingress_options.live_load(),
            dispatcher,
            schema,
            health,
        );

        Self { ingress_http }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        self.ingress_http.run().await
    }
}
