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
use restate_core::protobuf::metadata_proxy_svc::metadata_proxy_svc_server::MetadataProxySvcServer;
use tonic::codec::CompressionEncoding;

use restate_core::Identification;
use restate_core::TaskCenter;
use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::grpc::CoreNodeSvcHandler;
use restate_core::network::tonic_service_filter::{TonicServiceFilter, WaitForReady};
use restate_core::network::{ConnectionManager, NetworkServerBuilder};
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_server::NodeCtlSvcServer;
use restate_tracing_instrumentation::prometheus_metrics::Prometheus;
use restate_types::config::CommonOptions;
use restate_types::protobuf::common::NodeStatus;

use super::grpc_svc_handler::{MetadataProxySvcHandler, NodeCtlSvcHandler};
use super::pprof;
use crate::network_server::metrics::render_metrics;
use crate::network_server::state::NodeCtrlHandlerStateBuilder;

pub struct NetworkServer {}

impl NetworkServer {
    pub async fn run(
        connection_manager: ConnectionManager,
        mut server_builder: NetworkServerBuilder,
        options: CommonOptions,
        metadata_store_client: MetadataStoreClient,
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

        let (node_health, node_rpc_health) = TaskCenter::with_current(|tc| {
            (tc.health().node_status(), tc.health().node_rpc_status())
        });

        server_builder.register_grpc_service(
            NodeCtlSvcServer::new(NodeCtlSvcHandler::new(metadata_store_client.clone()))
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
            restate_core::protobuf::node_ctl_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder.register_grpc_service(
            MetadataProxySvcServer::new(MetadataProxySvcHandler::new(metadata_store_client))
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
            restate_core::protobuf::metadata_proxy_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder.register_grpc_service(
            TonicServiceFilter::new(
                CoreNodeSvcHandler::new(connection_manager).into_server(),
                WaitForReady::new(node_health, NodeStatus::Alive),
            ),
            restate_core::network::protobuf::core_node_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder
            .run(node_rpc_health, &options.bind_address.unwrap())
            .await?;

        Ok(())
    }
}

pub async fn report_health() -> Json<Identification> {
    Json(Identification::get())
}
