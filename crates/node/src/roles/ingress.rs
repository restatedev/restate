// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::{Networking, TransportConnect};
use restate_core::partitions::PartitionRouting;
use restate_core::worker_api::PartitionProcessorInvocationClient;
use restate_core::{TaskCenter, TaskKind};
use restate_ingress_http::{HyperServerIngress, InvocationClientRequestDispatcher};
use restate_types::config::IngressOptions;
use restate_types::health::HealthStatus;
use restate_types::live::{BoxLiveLoad, Live};
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::common::IngressStatus;
use restate_types::schema::Schema;

type IngressHttp<T> = HyperServerIngress<
    Schema,
    InvocationClientRequestDispatcher<PartitionProcessorInvocationClient<T>>,
>;

pub struct IngressRole<T> {
    ingress_http: IngressHttp<T>,
}

impl<T: TransportConnect> IngressRole<T> {
    pub fn create(
        mut ingress_options: BoxLiveLoad<IngressOptions>,
        health: HealthStatus<IngressStatus>,
        networking: Networking<T>,
        schema: Live<Schema>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
    ) -> Self {
        let dispatcher = InvocationClientRequestDispatcher::new(
            PartitionProcessorInvocationClient::new(networking, partition_table, partition_routing),
        );
        let ingress_http = HyperServerIngress::from_options(
            ingress_options.live_load(),
            dispatcher,
            schema,
            health,
        );

        Self { ingress_http }
    }

    pub fn start(self) -> Result<(), anyhow::Error> {
        TaskCenter::spawn(
            TaskKind::HttpIngressRole,
            "ingress-http",
            self.ingress_http.run(),
        )?;

        Ok(())
    }
}
