// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;

use http::Request;
use hyper::body::Incoming;
use hyper_util::service::TowerToHyperService;
use tonic::body::boxed;
use tonic::service::Routes;
use tower::ServiceExt;
use tower_http::trace::TraceLayer;
use tracing::debug;

use restate_types::health::HealthStatus;
use restate_types::net::BindAddress;
use restate_types::protobuf::common::NodeStatus;

use super::multiplex::MultiplexService;
use super::net_util::run_hyper_server;

#[derive(Debug, Default)]
pub struct NetworkServerBuilder {
    grpc_descriptors: Vec<&'static [u8]>,
    grpc_routes: Option<Routes>,
}

impl NetworkServerBuilder {
    pub fn register_grpc_service<S>(
        &mut self,
        svc: S,
        file_descriptor_set: &'static [u8],
    ) -> &mut Self
    where
        S: tower::Service<
                Request<tonic::body::BoxBody>,
                Response = http::Response<tonic::body::BoxBody>,
                Error = Infallible,
            > + tonic::server::NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        let current_routes = self.grpc_routes.take().unwrap_or_default();
        self.grpc_descriptors.push(file_descriptor_set);
        debug!(svc = S::NAME, "Registering gRPC service to node-rpc-server");
        self.grpc_routes = Some(current_routes.add_service(svc));
        self
    }

    pub async fn run(
        self,
        node_health: HealthStatus<NodeStatus>,
        axum_router: axum::routing::Router,
        bind_address: &BindAddress,
    ) -> Result<(), anyhow::Error> {
        // Trace layer
        let span_factory = tower_http::trace::DefaultMakeSpan::new()
            .include_headers(true)
            .level(tracing::Level::ERROR);

        let axum_router = axum_router
            .layer(TraceLayer::new_for_http().make_span_with(span_factory.clone()))
            .fallback(handler_404);

        let mut reflection_service_builder = tonic_reflection::server::Builder::configure();
        for descriptor in self.grpc_descriptors {
            reflection_service_builder =
                reflection_service_builder.register_encoded_file_descriptor_set(descriptor);
        }

        let server_builder = tonic::transport::Server::builder()
            .layer(TraceLayer::new_for_grpc().make_span_with(span_factory))
            .add_routes(self.grpc_routes.unwrap_or_default())
            .add_service(reflection_service_builder.build_v1()?);

        // Multiplex both grpc and http based on content-type
        let service = TowerToHyperService::new(
            MultiplexService::new(axum_router, server_builder.into_service())
                .map_request(|req: Request<Incoming>| req.map(boxed)),
        );

        run_hyper_server(
            bind_address,
            service,
            "node-rpc-server",
            || node_health.update(NodeStatus::Alive),
            || node_health.update(NodeStatus::ShuttingDown),
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
