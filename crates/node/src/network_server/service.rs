// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::routing::get;
use http::Request;
use hyper::body::Incoming;
use hyper_util::service::TowerToHyperService;
use tonic::body::boxed;
use tonic::codec::CompressionEncoding;
use tower::ServiceExt;
use tower_http::trace::TraceLayer;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_server::ClusterCtrlSvcServer;
use restate_admin::cluster_controller::ClusterControllerHandle;
use restate_bifrost::Bifrost;
use restate_core::network::net_util::run_hyper_server;
use restate_core::network::protobuf::node_svc::node_svc_server::NodeSvcServer;
use restate_core::network::{ConnectionManager, GrpcConnector};
use restate_core::{task_center, MetadataWriter};
use restate_metadata_store::MetadataStoreClient;
use restate_storage_query_datafusion::context::QueryContext;
use restate_types::config::CommonOptions;
use restate_types::health::Health;
use restate_types::protobuf::common::NodeStatus;

use crate::network_server::handler;
use crate::network_server::handler::cluster_ctrl::ClusterCtrlSvcHandler;
use crate::network_server::handler::node::NodeSvcHandler;
use crate::network_server::metrics::install_global_prometheus_recorder;
use crate::network_server::multiplex::MultiplexService;
use crate::network_server::state::NodeCtrlHandlerStateBuilder;

pub struct NetworkServer {
    health: Health,
    connection_manager: ConnectionManager<GrpcConnector>,
    worker_deps: Option<WorkerDependencies>,
    cluster_controller_deps: Option<ClusterControllerDependencies>,
}

impl NetworkServer {
    pub fn new(
        health: Health,
        connection_manager: ConnectionManager<GrpcConnector>,
        worker_deps: Option<WorkerDependencies>,
        cluster_controller_deps: Option<ClusterControllerDependencies>,
    ) -> Self {
        Self {
            health,
            connection_manager,
            worker_deps,
            cluster_controller_deps,
        }
    }

    pub async fn run(self, options: CommonOptions) -> Result<(), anyhow::Error> {
        let tc = task_center();
        // Configure Metric Exporter
        let mut state_builder = NodeCtrlHandlerStateBuilder::default();
        state_builder.task_center(tc.clone());

        if !options.disable_prometheus {
            state_builder.prometheus_handle(Some(install_global_prometheus_recorder(&options)));
        }

        let shared_state = state_builder.build().expect("should be infallible");

        // Trace layer
        let span_factory = tower_http::trace::DefaultMakeSpan::new()
            .include_headers(true)
            .level(tracing::Level::ERROR);

        // -- HTTP service (for prometheus et al.)
        let router = axum::Router::new()
            .route("/metrics", get(handler::render_metrics))
            .with_state(shared_state)
            .layer(TraceLayer::new_for_http().make_span_with(span_factory.clone()))
            .fallback(handler_404);

        // -- GRPC Service Setup
        let mut reflection_service_builder = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(
                restate_core::network::protobuf::node_svc::FILE_DESCRIPTOR_SET,
            )
            .register_encoded_file_descriptor_set(restate_types::protobuf::FILE_DESCRIPTOR_SET);

        if self.cluster_controller_deps.is_some() {
            reflection_service_builder = reflection_service_builder
                .register_encoded_file_descriptor_set(
                    restate_admin::cluster_controller::protobuf::FILE_DESCRIPTOR_SET,
                );
        }

        let cluster_controller_service =
            self.cluster_controller_deps.map(|cluster_controller_deps| {
                ClusterCtrlSvcServer::new(ClusterCtrlSvcHandler::new(cluster_controller_deps))
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip)
            });

        let node_status = self.health.node_status();
        let server_builder = tonic::transport::Server::builder()
            .layer(TraceLayer::new_for_grpc().make_span_with(span_factory))
            .add_service(
                NodeSvcServer::new(NodeSvcHandler::new(
                    tc,
                    options.cluster_name().to_owned(),
                    options.roles,
                    self.health,
                    self.worker_deps,
                    self.connection_manager,
                ))
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
            )
            .add_optional_service(cluster_controller_service)
            .add_service(reflection_service_builder.build_v1()?);

        // Multiplex both grpc and http based on content-type
        let service = TowerToHyperService::new(
            MultiplexService::new(router, server_builder.into_service())
                .map_request(|req: Request<Incoming>| req.map(boxed)),
        );

        run_hyper_server(
            &options.bind_address,
            service,
            "node-grpc",
            || node_status.update(NodeStatus::Alive),
            || node_status.update(NodeStatus::ShuttingDown),
        )
        .await?;

        Ok(())
    }
}

// handle 404
async fn handler_404() -> (axum::http::StatusCode, &'static str) {
    (
        axum::http::StatusCode::NOT_FOUND,
        "Are you lost? Maybe visit https://restate.dev instead!",
    )
}

pub struct WorkerDependencies {
    pub query_context: QueryContext,
}

impl WorkerDependencies {
    pub fn new(query_context: QueryContext) -> Self {
        WorkerDependencies { query_context }
    }
}

pub struct ClusterControllerDependencies {
    pub cluster_controller_handle: ClusterControllerHandle,
    pub metadata_store_client: MetadataStoreClient,
    pub bifrost_handle: Bifrost,
    pub metadata_writer: MetadataWriter,
}

impl ClusterControllerDependencies {
    pub fn new(
        cluster_controller_handle: ClusterControllerHandle,
        metadata_store_client: MetadataStoreClient,
        metadata_writer: MetadataWriter,
        bifrost_handle: Bifrost,
    ) -> Self {
        ClusterControllerDependencies {
            cluster_controller_handle,
            metadata_store_client,
            bifrost_handle,
            metadata_writer,
        }
    }
}
