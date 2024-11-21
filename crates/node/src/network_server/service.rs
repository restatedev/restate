// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::routing::get;
use tonic::codec::CompressionEncoding;

use restate_core::network::protobuf::node_svc::node_svc_server::NodeSvcServer;
use restate_core::network::{ConnectionManager, NetworkServerBuilder, TransportConnect};
use restate_core::task_center;
use restate_types::config::CommonOptions;
use restate_types::health::Health;

use crate::network_server::metrics::{install_global_prometheus_recorder, render_metrics};
use crate::network_server::state::NodeCtrlHandlerStateBuilder;

use super::grpc_svc_handler::NodeSvcHandler;

pub struct NetworkServer {}

impl NetworkServer {
    pub async fn run<T: TransportConnect>(
        health: Health,
        connection_manager: ConnectionManager<T>,
        mut server_builder: NetworkServerBuilder,
        options: CommonOptions,
    ) -> Result<(), anyhow::Error> {
        let tc = task_center();
        // Configure Metric Exporter
        let mut state_builder = NodeCtrlHandlerStateBuilder::default();
        state_builder.task_center(tc.clone());

        if !options.disable_prometheus {
            state_builder.prometheus_handle(Some(install_global_prometheus_recorder(&options)));
        }

        let shared_state = state_builder.build().expect("should be infallible");

        // -- HTTP service (for prometheus et al.)
        let axum_router = axum::Router::new()
            .route("/metrics", get(render_metrics))
            .with_state(shared_state);

        let node_health = health.node_status();

        server_builder.register_grpc_service(
            NodeSvcServer::new(NodeSvcHandler::new(
                tc,
                options.cluster_name().to_owned(),
                options.roles,
                health,
                connection_manager,
            ))
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip),
            restate_types::protobuf::FILE_DESCRIPTOR_SET,
        );

        server_builder
            .run(node_health, axum_router, &options.bind_address.unwrap())
            .await?;

        Ok(())
    }
}
