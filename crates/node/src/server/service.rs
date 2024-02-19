// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::body::BoxBody;
use std::io;
use std::net::{AddrParseError, SocketAddr};

use axum::routing::get;
use codederror::CodedError;
use hyper::server::accept::Accept;
use hyper::server::conn::AddrIncoming;
use hyper::service::Service;
use serde::de::StdError;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UnixListener;
use tonic::codegen::tokio_stream::wrappers::UnixListenerStream;
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
use restate_types::nodes_config::NetworkAddress;
use restate_worker::{SubscriptionControllerHandle, WorkerCommandSender};

use crate::server::multiplex::MultiplexService;
use crate::server::options::Options;
use crate::server::state::HandlerStateBuilder;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("failed binding to address '{address}' specified in 'server.bind_address': {source}")]
    #[code(restate_errors::RT0004)]
    TcpBinding {
        address: SocketAddr,
        #[source]
        source: hyper::Error,
    },
    #[error("failed opening uds '{uds_path}' specified in 'server.bind_address': {source}")]
    #[code(unknown)]
    UdsBinding {
        uds_path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed parsing dns name specified in server.bind_address': {0}")]
    #[code(unknown)]
    DnsParsing(AddrParseError),
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

        match self.opts.bind_address {
            NetworkAddress::Uds(uds_path) => {
                let unix_listener =
                    UnixListener::bind(&uds_path).map_err(|err| Error::UdsBinding {
                        uds_path: uds_path.clone(),
                        source: err,
                    })?;
                let acceptor =
                    hyper::server::accept::from_stream(UnixListenerStream::new(unix_listener));

                info!(
                    uds.path = %uds_path,
                    "Node server listening");

                Self::run_server(service, acceptor).await?
            }
            NetworkAddress::TcpSocketAddr(socket_addr) => {
                Self::run_tcp_server(service, socket_addr).await?
            }
            NetworkAddress::DnsName(dns_name) => {
                let socket_addr = dns_name.parse().map_err(Error::DnsParsing)?;
                Self::run_tcp_server(service, socket_addr).await?
            }
        }

        Ok(())
    }

    pub fn address(&self) -> &NetworkAddress {
        &self.opts.bind_address
    }

    async fn run_tcp_server<S>(service: S, socket_addr: SocketAddr) -> Result<(), Error>
    where
        S: Service<http::Request<hyper::Body>, Response = http::Response<BoxBody>>
            + Send
            + Clone
            + 'static,
        S::Error: Into<Box<dyn StdError + Send + Sync>>,
        S::Future: Send,
    {
        let acceptor = AddrIncoming::bind(&socket_addr).map_err(|err| Error::TcpBinding {
            address: socket_addr,
            source: err,
        })?;

        info!(
            net.host.addr = %acceptor.local_addr().ip(),
            net.host.port = %acceptor.local_addr().port(),
            "Node server listening");

        Self::run_server(service, acceptor).await
    }

    async fn run_server<S, Conn, Err>(
        service: S,
        acceptor: impl Accept<Conn = Conn, Error = Err>,
    ) -> Result<(), Error>
    where
        S: Service<http::Request<hyper::Body>, Response = http::Response<BoxBody>>
            + Send
            + Clone
            + 'static,
        S::Error: Into<Box<dyn StdError + Send + Sync>>,
        S::Future: Send,
        Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        Err: Into<Box<dyn StdError + Send + Sync>>,
    {
        let server = hyper::Server::builder(acceptor).serve(tower::make::Shared::new(service));

        // Wait server graceful shutdown
        server
            .with_graceful_shutdown(cancellation_watcher())
            .await
            .map_err(Error::Running)
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
