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
use futures::FutureExt;
use restate_bifrost::Bifrost;
use restate_cluster_controller::ClusterControllerHandle;
use restate_storage_rocksdb::RocksDBStorage;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::server::handler;
use crate::server::handler::cluster_controller::ClusterControllerHandler;
use crate::server::handler::node_ctrl::NodeCtrlHandler;
use crate::server::handler::worker::WorkerHandler;
use crate::server::metrics::install_global_prometheus_recorder;
use restate_node_services::cluster_controller::cluster_controller_server::ClusterControllerServer;
use restate_node_services::node_ctrl::node_ctrl_server::NodeCtrlServer;
use restate_node_services::worker::worker_server::WorkerServer;
use restate_node_services::{cluster_controller, node_ctrl, worker};

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
    worker: Option<(RocksDBStorage, Bifrost)>,
    cluster_controller: Option<ClusterControllerHandle>,
}

impl NodeServer {
    pub fn new(
        opts: Options,
        worker: Option<(RocksDBStorage, Bifrost)>,
        cluster_controller: Option<ClusterControllerHandle>,
    ) -> Self {
        Self {
            opts,
            worker,
            cluster_controller,
        }
    }

    pub async fn run(self, drain: drain::Watch) -> Result<(), Error> {
        // Configure Metric Exporter
        let mut state_builder = HandlerStateBuilder::default();

        if let Some((rocksdb, _)) = self.worker.as_ref() {
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
                .register_encoded_file_descriptor_set(cluster_controller::FILE_DESCRIPTOR_SET);
        }

        if self.worker.is_some() {
            reflection_service_builder = reflection_service_builder
                .register_encoded_file_descriptor_set(worker::FILE_DESCRIPTOR_SET);
        }

        let mut server_builder = tonic::transport::Server::builder()
            .layer(TraceLayer::new_for_grpc().make_span_with(span_factory))
            .add_service(NodeCtrlServer::new(NodeCtrlHandler::new()))
            .add_service(reflection_service_builder.build()?);

        if self.cluster_controller.is_some() {
            server_builder = server_builder
                .add_service(ClusterControllerServer::new(ClusterControllerHandler::new()));
        }

        if let Some((_, bifrost)) = self.worker {
            server_builder =
                server_builder.add_service(WorkerServer::new(WorkerHandler::new(bifrost)));
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
        server
            .with_graceful_shutdown(drain.signaled().map(|_| ()))
            .await
            .map_err(Error::Running)
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
