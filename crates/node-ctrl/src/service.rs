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
use http::Uri;
use restate_bifrost::Bifrost;
use restate_cluster_controller::ClusterControllerHandle;
use restate_storage_rocksdb::RocksDBStorage;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::handler::cluster_controller::ClusterControllerHandler;
use crate::handler::node_ctrl::NodeCtrlHandler;
use restate_node_ctrl_proto::cluster_controller::cluster_controller_server::ClusterControllerServer;
use restate_node_ctrl_proto::node_ctrl::node_ctrl_server::NodeCtrlServer;
use restate_node_ctrl_proto::{cluster_controller, node_ctrl};

use crate::multiplex::MultiplexService;
use crate::state::HandlerStateBuilder;
use crate::Options;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("failed binding to address '{address}' specified in 'node_ctrl.bind_address'")]
    #[code(restate_errors::RT0004)]
    Binding {
        address: SocketAddr,
        #[source]
        source: hyper::Error,
    },
    #[error("error while running node-ctrl service: {0}")]
    #[code(unknown)]
    Running(hyper::Error),
    #[error("error while running node-ctrl server grpc reflection service: {0}")]
    #[code(unknown)]
    Grpc(#[from] tonic_reflection::server::Error),
}

pub struct NodeCtrlService {
    opts: Options,
    rocksdb_storage: Option<RocksDBStorage>,
    bifrost: Bifrost,
    cluster_controller: Option<ClusterControllerHandle>,
}

impl NodeCtrlService {
    pub fn new(
        opts: Options,
        rocksdb_storage: Option<RocksDBStorage>,
        bifrost: Bifrost,
        cluster_controller: Option<ClusterControllerHandle>,
    ) -> Self {
        Self {
            opts,
            rocksdb_storage,
            bifrost,
            cluster_controller,
        }
    }

    pub async fn run(self, drain: drain::Watch) -> Result<(), Error> {
        // Configure Metric Exporter
        let mut state_builder = HandlerStateBuilder::default();
        state_builder.rocksdb_storage(self.rocksdb_storage);
        state_builder.bifrost(self.bifrost);

        if !self.opts.disable_prometheus {
            state_builder.prometheus_handle(Some(
                crate::metrics::install_global_prometheus_recorder(&self.opts),
            ));
        }

        let shared_state = state_builder.build().unwrap();
        // Trace layer
        let span_factory = tower_http::trace::DefaultMakeSpan::new()
            .include_headers(true)
            .level(tracing::Level::ERROR);

        // -- HTTP service (for prometheus et al.)
        let router = axum::Router::new()
            .route("/metrics", get(crate::handler::render_metrics))
            .route("/rocksdb-stats", get(crate::handler::rocksdb_stats))
            .with_state(shared_state.clone())
            .layer(TraceLayer::new_for_http().make_span_with(span_factory.clone()))
            .fallback(handler_404);

        // -- GRPC Service Setup
        let reflection_service_builder = if self.cluster_controller.is_some() {
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(cluster_controller::FILE_DESCRIPTOR_SET)
        } else {
            tonic_reflection::server::Builder::configure()
        };

        let tonic_reflection_service = reflection_service_builder
            .register_encoded_file_descriptor_set(node_ctrl::FILE_DESCRIPTOR_SET)
            .build()?;

        // Build the NodeCtrl grpc service
        let grpc = if self.cluster_controller.is_some() {
            tonic::transport::Server::builder()
                .layer(TraceLayer::new_for_grpc().make_span_with(span_factory))
                .add_service(NodeCtrlServer::new(NodeCtrlHandler::new(shared_state)))
                .add_service(ClusterControllerServer::new(ClusterControllerHandler::new()))
                .add_service(tonic_reflection_service)
                .into_service()
        } else {
            tonic::transport::Server::builder()
                .layer(TraceLayer::new_for_grpc().make_span_with(span_factory))
                .add_service(NodeCtrlServer::new(NodeCtrlHandler::new(shared_state)))
                .add_service(tonic_reflection_service)
                .into_service()
        };

        // Multiplex both grpc and http based on content-type
        let service = MultiplexService::new(router, grpc);

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
            "Node-ctrl service listening"
        );

        // Wait server graceful shutdown
        server
            .with_graceful_shutdown(drain.signaled().map(|_| ()))
            .await
            .map_err(Error::Running)
    }

    pub fn endpoint(&self) -> Uri {
        Uri::builder()
            .scheme("http")
            .authority(self.opts.bind_address.to_string())
            .path_and_query("/")
            .build()
            .expect("should be valid uri")
    }
}

// handle 404
async fn handler_404() -> (http::StatusCode, &'static str) {
    (
        http::StatusCode::NOT_FOUND,
        "Are you lost? Maybe visit https://restate.dev instead!",
    )
}
