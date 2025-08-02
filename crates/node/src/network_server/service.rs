// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::Json;
use axum::routing::{MethodFilter, get, on};

use restate_core::TaskCenter;
use restate_core::network::grpc::CoreNodeSvcHandler;
use restate_core::network::{ConnectionManager, NetworkServerBuilder};
use restate_core::{Identification, MetadataWriter};
use restate_tracing_instrumentation::prometheus_metrics::Prometheus;
use restate_types::config::Configuration;

use super::grpc_svc_handler::{MetadataProxySvcHandler, NodeCtlSvcHandler};
use super::pprof;
use crate::network_server::metrics::render_metrics;
use crate::network_server::state::NodeCtrlHandlerStateBuilder;

pub struct NetworkServer {}

impl NetworkServer {
    pub async fn run(
        connection_manager: ConnectionManager,
        mut server_builder: NetworkServerBuilder,
        metadata_writer: MetadataWriter,
        prometheus: Prometheus,
    ) -> Result<(), anyhow::Error> {
        // Configure Metric Exporter
        let mut state_builder = NodeCtrlHandlerStateBuilder::default();
        state_builder.task_center(TaskCenter::current());

        state_builder.prometheus_handle(prometheus.into());

        let shared_state = state_builder.build().expect("should be infallible");

        let post_or_put = MethodFilter::POST.or(MethodFilter::PUT);

        // -- HTTP service (for prometheus et al.)
        let axum_router = axum::Router::new()
            .route("/health", get(report_health))
            .route("/metrics", get(render_metrics))
            .route("/debug/pprof/heap", get(pprof::heap))
            .route(
                "/debug/pprof/heap/activate",
                on(post_or_put, pprof::activate_heap),
            )
            .route(
                "/debug/pprof/heap/deactivate",
                on(post_or_put, pprof::deactivate_heap),
            )
            .with_state(shared_state);

        server_builder.register_axum_routes(axum_router);

        let node_rpc_health = TaskCenter::with_current(|tc| tc.health().node_rpc_status());

        server_builder.register_grpc_service(
            MetadataProxySvcHandler::new(metadata_writer.raw_metadata_store_client().clone())
                .into_server(&Configuration::pinned().networking),
            restate_metadata_store::protobuf::metadata_proxy_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder.register_grpc_service(
            NodeCtlSvcHandler::new(metadata_writer)
                .into_server(&Configuration::pinned().networking),
            restate_core::protobuf::node_ctl_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder.register_grpc_service(
            CoreNodeSvcHandler::new(connection_manager)
                .into_server(&Configuration::pinned().networking),
            restate_core::network::protobuf::core_node_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder
            .run(
                node_rpc_health,
                Configuration::pinned()
                    .common
                    .bind_address
                    .as_ref()
                    .unwrap(),
            )
            .await?;

        Ok(())
    }
}

pub async fn report_health() -> Json<Identification> {
    Json(Identification::get())
}
