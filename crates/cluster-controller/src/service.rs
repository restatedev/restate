// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::handler::Handler;
use crate::options::Options;
use crate::proto;
use crate::proto::cluster_controller_server::ClusterControllerServer;
use codederror::CodedError;
use futures::FutureExt;
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;
use tracing::info;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error(
        "failed binding to address '{address}' specified in 'cluster_controller.bind_address'"
    )]
    #[code(restate_errors::RT0004)]
    Binding {
        address: SocketAddr,
        #[source]
        source: hyper::Error,
    },
    #[error("error while running controller service: {0}")]
    #[code(unknown)]
    Running(hyper::Error),
    #[error("error while running controller server grpc reflection service: {0}")]
    #[code(unknown)]
    Grpc(#[from] tonic_reflection::server::Error),
}

#[derive(Debug)]
pub struct Service {
    options: Options,
}

impl Service {
    pub fn new(options: Options) -> Self {
        Service { options }
    }

    pub async fn run(self, shutdown_watch: drain::Watch) -> Result<(), Error> {
        // Trace layer
        let span_factory = tower_http::trace::DefaultMakeSpan::new()
            .include_headers(true)
            .level(tracing::Level::ERROR);

        // -- GRPC Service Setup
        let tonic_reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
            .build()?;

        // Build the Controller grpc service
        let grpc = tonic::transport::Server::builder()
            .layer(TraceLayer::new_for_grpc().make_span_with(span_factory))
            .add_service(ClusterControllerServer::new(Handler::new()))
            .add_service(tonic_reflection_service)
            .into_service();

        // Bind and serve
        let server = hyper::Server::try_bind(&self.options.bind_address)
            .map_err(|err| Error::Binding {
                address: self.options.bind_address,
                source: err,
            })?
            .serve(tower::make::Shared::new(grpc));

        info!(
            net.host.addr = %server.local_addr().ip(),
            net.host.port = %server.local_addr().port(),
            "Heimdall service listening"
        );

        // Wait server graceful shutdown
        server
            .with_graceful_shutdown(shutdown_watch.signaled().map(|_| ()))
            .await
            .map_err(Error::Running)
    }
}
