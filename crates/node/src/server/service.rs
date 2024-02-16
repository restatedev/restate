// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use axum::routing::get;
use codederror::CodedError;
use tower_http::trace::TraceLayer;
use tracing::info;

use restate_bifrost::Bifrost;
use restate_cluster_controller::ClusterControllerHandle;
use restate_meta::FileMetaReader;
use restate_storage_rocksdb::RocksDBStorage;
use restate_task_center::cancellation_watcher;

use crate::server::handler;
use crate::server::handler::cluster_controller::ClusterControllerHandler;
use crate::server::handler::metadata::MetadataHandler;
use crate::server::handler::node_ctrl::NodeCtrlHandler;
use crate::server::handler::worker::WorkerHandler;
// TODO cleanup
use crate::server::metrics::install_global_prometheus_recorder;
use restate_node_services::cluster_controller::cluster_controller_svc_server::ClusterControllerSvcServer;
use restate_node_services::metadata::metadata_svc_server::MetadataSvcServer;
use restate_node_services::node_ctrl::node_ctrl_svc_server::NodeCtrlSvcServer;
use restate_node_services::worker::worker_svc_server::WorkerSvcServer;
use restate_node_services::{cluster_controller, metadata, node_ctrl, worker};
use restate_schema_impl::Schemas;
use restate_storage_query_datafusion::context::QueryContext;
use restate_worker::{SubscriptionControllerHandle, WorkerCommandSender};

use crate::server::multiplex::MultiplexService;
use crate::server::options::Options;
use crate::server::state::HandlerStateBuilder;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("failed binding to address '{address}' specified in 'server.bind_address'")]
    #[code(restate_errors::RT0004)]
    Binding {
        address: SocketAddr,
        #[source]
        source: hyper::Error,
    },
    #[error("error while running server service: {0}")]
    #[code(unknown)]
    Running(hyper::Error),
    #[error("error while running server server grpc reflection service: {0}")]
    #[code(unknown)]
    Grpc(#[from] tonic_reflection::server::Error),
}

pub struct NodeServer {
    opts: Options,
    worker: Option<WorkerDependencies>,
    cluster_controller: Option<ClusterControllerDependencies>,
}

impl NodeServer {
    pub fn new(
        opts: Options,
        worker: Option<WorkerDependencies>,
        cluster_controller: Option<ClusterControllerDependencies>,
    ) -> Self {
        Self {
            opts,
            worker,
            cluster_controller,
        }
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        // Configure Metric Exporter
        let mut state_builder = HandlerStateBuilder::default();

        if let Some(WorkerDependencies { rocksdb, .. }) = self.worker.as_ref() {
            state_builder.rocksdb_storage(Some(rocksdb.clone()));
        }

        if !self.opts.disable_prometheus {
            state_builder.prometheus_handle(Some(install_global_prometheus_recorder(&self.opts)));
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
            .register_encoded_file_descriptor_set(node_ctrl::FILE_DESCRIPTOR_SET);

        if self.cluster_controller.is_some() {
            reflection_service_builder = reflection_service_builder
                .register_encoded_file_descriptor_set(cluster_controller::FILE_DESCRIPTOR_SET)
                .register_encoded_file_descriptor_set(metadata::FILE_DESCRIPTOR_SET);
        }

        if self.worker.is_some() {
            reflection_service_builder = reflection_service_builder
                .register_encoded_file_descriptor_set(worker::FILE_DESCRIPTOR_SET);
        }

        let mut server_builder = tonic::transport::Server::builder()
            .layer(TraceLayer::new_for_grpc().make_span_with(span_factory))
            .add_service(NodeCtrlSvcServer::new(NodeCtrlHandler::new()))
            .add_service(reflection_service_builder.build()?);

        if let Some(ClusterControllerDependencies { schema_reader, .. }) = self.cluster_controller {
            server_builder = server_builder
                .add_service(ClusterControllerSvcServer::new(
                    ClusterControllerHandler::new(),
                ))
                .add_service(MetadataSvcServer::new(MetadataHandler::new(schema_reader)));
        }

        if let Some(WorkerDependencies {
            bifrost,
            worker_cmd_tx,
            query_context,
            schemas,
            subscription_controller,
            ..
        }) = self.worker
        {
            server_builder = server_builder.add_service(WorkerSvcServer::new(WorkerHandler::new(
                bifrost,
                worker_cmd_tx,
                query_context,
                schemas,
                subscription_controller,
            )));
        }

        // Multiplex both grpc and http based on content-type
        let service = MultiplexService::new(router, server_builder.into_service());

        // Bind and serve
        let server = hyper::Server::try_bind(&self.opts.bind_address)
            .map_err(|err| Error::Binding {
                address: self.opts.bind_address,
                source: err,
            })?
            .serve(tower::make::Shared::new(service));

        info!(
            net.host.addr = %server.local_addr().ip(),
            net.host.port = %server.local_addr().port(),
            "Node server listening"
        );

        // Wait server graceful shutdown
        Ok(server
            .with_graceful_shutdown(cancellation_watcher())
            .await
            .map_err(Error::Running)?)
    }

    pub fn port(&self) -> u16 {
        self.opts.bind_address.port()
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
    rocksdb: RocksDBStorage,
    bifrost: Bifrost,
    worker_cmd_tx: WorkerCommandSender,
    query_context: QueryContext,
    schemas: Schemas,
    subscription_controller: Option<SubscriptionControllerHandle>,
}

impl WorkerDependencies {
    pub fn new(
        rocksdb: RocksDBStorage,
        bifrost: Bifrost,
        worker_cmd_tx: WorkerCommandSender,
        query_context: QueryContext,
        schemas: Schemas,
        subscription_controller: Option<SubscriptionControllerHandle>,
    ) -> Self {
        WorkerDependencies {
            rocksdb,
            bifrost,
            worker_cmd_tx,
            query_context,
            schemas,
            subscription_controller,
        }
    }
}

pub struct ClusterControllerDependencies {
    _cluster_controller_handle: ClusterControllerHandle,
    schema_reader: FileMetaReader,
}

impl ClusterControllerDependencies {
    pub fn new(
        cluster_controller_handle: ClusterControllerHandle,
        schema_reader: FileMetaReader,
    ) -> Self {
        ClusterControllerDependencies {
            _cluster_controller_handle: cluster_controller_handle,
            schema_reader,
        }
    }
}
