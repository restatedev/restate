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
use tower_http::trace::TraceLayer;

use restate_cluster_controller::ClusterControllerHandle;
use restate_core::{cancellation_watcher, task_center};
use restate_grpc_util::run_hyper_server;
use restate_meta::FileMetaReader;
use restate_metadata_store::MetadataStoreClient;
use restate_network::ConnectionManager;
use restate_node_protocol::{common, node};
use restate_node_services::cluster_ctrl;
use restate_node_services::cluster_ctrl::cluster_ctrl_svc_server::ClusterCtrlSvcServer;
use restate_node_services::node_svc::node_svc_server::NodeSvcServer;
use restate_schema_impl::Schemas;
use restate_storage_query_datafusion::context::QueryContext;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::config::CommonOptions;
use restate_worker::SubscriptionControllerHandle;

use crate::network_server::handler;
use crate::network_server::handler::cluster_ctrl::ClusterCtrlSvcHandler;
use crate::network_server::handler::node::NodeSvcHandler;
use crate::network_server::metrics::install_global_prometheus_recorder;
use crate::network_server::multiplex::MultiplexService;
use crate::network_server::state::NodeCtrlHandlerStateBuilder;

pub struct NetworkServer {
    connection_manager: ConnectionManager,
    worker_deps: Option<WorkerDependencies>,
    admin_deps: Option<AdminDependencies>,
}

impl NetworkServer {
    pub fn new(
        connection_manager: ConnectionManager,
        worker_deps: Option<WorkerDependencies>,
        admin_deps: Option<AdminDependencies>,
    ) -> Self {
        Self {
            connection_manager,
            worker_deps,
            admin_deps,
        }
    }

    pub async fn run(self, options: CommonOptions) -> Result<(), anyhow::Error> {
        // Configure Metric Exporter
        let mut state_builder = NodeCtrlHandlerStateBuilder::default();

        if let Some(WorkerDependencies { rocksdb, .. }) = self.worker_deps.as_ref() {
            state_builder.rocksdb_storage(Some(rocksdb.clone()));
        }

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
            .route("/rocksdb-stats", get(handler::rocksdb_stats))
            .with_state(shared_state)
            .layer(TraceLayer::new_for_http().make_span_with(span_factory.clone()))
            .fallback(handler_404);

        // -- GRPC Service Setup
        let mut reflection_service_builder = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(node::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(common::FILE_DESCRIPTOR_SET);

        if self.admin_deps.is_some() {
            reflection_service_builder = reflection_service_builder
                .register_encoded_file_descriptor_set(cluster_ctrl::FILE_DESCRIPTOR_SET);
        }

        let cluster_controller_service = self
            .admin_deps
            .map(|admin_deps| ClusterCtrlSvcServer::new(ClusterCtrlSvcHandler::new(admin_deps)));

        let server_builder = tonic::transport::Server::builder()
            .layer(TraceLayer::new_for_grpc().make_span_with(span_factory))
            .add_service(NodeSvcServer::new(NodeSvcHandler::new(
                task_center(),
                self.worker_deps,
                self.connection_manager,
            )))
            .add_optional_service(cluster_controller_service)
            .add_service(reflection_service_builder.build()?);

        // Multiplex both grpc and http based on content-type
        let service = MultiplexService::new(router, server_builder.into_service());

        run_hyper_server(
            &options.bind_address,
            service,
            cancellation_watcher(),
            "node-grpc",
        )
        .await?;

        Ok(())
    }
}

// handle 404
async fn handler_404() -> (http::StatusCode, &'static str) {
    (
        http::StatusCode::NOT_FOUND,
        "Are you lost? Maybe visit https://restate.dev instead!",
    )
}

pub struct WorkerDependencies {
    pub rocksdb: RocksDBStorage,
    pub query_context: QueryContext,
    pub schemas: Schemas,
    pub subscription_controller: Option<SubscriptionControllerHandle>,
}

impl WorkerDependencies {
    pub fn new(
        rocksdb: RocksDBStorage,
        query_context: QueryContext,
        schemas: Schemas,
        subscription_controller: Option<SubscriptionControllerHandle>,
    ) -> Self {
        WorkerDependencies {
            rocksdb,
            query_context,
            schemas,
            subscription_controller,
        }
    }
}

pub struct AdminDependencies {
    pub _cluster_controller_handle: ClusterControllerHandle,
    pub schema_reader: FileMetaReader,
    pub metadata_store_client: MetadataStoreClient,
}

impl AdminDependencies {
    pub fn new(
        cluster_controller_handle: ClusterControllerHandle,
        schema_reader: FileMetaReader,
        metadata_store_client: MetadataStoreClient,
    ) -> Self {
        AdminDependencies {
            _cluster_controller_handle: cluster_controller_handle,
            schema_reader,
            metadata_store_client,
        }
    }
}
