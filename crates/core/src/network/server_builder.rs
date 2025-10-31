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
use hyper_util::service::TowerToHyperService;
use tonic::service::Routes;
use tower_http::trace::{DefaultOnFailure, TraceLayer};
use tracing::{Level, debug};

use restate_ty::protobuf::NodeRpcStatus;
use restate_types::health::HealthStatus;
use restate_types::net::address::FabricPort;
use restate_types::net::listener::{AddressBook, Listeners};

use super::net_util::run_hyper_server;

pub struct NetworkServerBuilder {
    grpc_descriptors: Vec<&'static [u8]>,
    grpc_routes: Option<Routes>,
    axum_router: Option<axum::routing::Router>,
    listeners: Listeners<FabricPort>,
}

impl NetworkServerBuilder {
    pub fn new(address_book: &mut AddressBook) -> Self {
        Self {
            grpc_descriptors: Default::default(),
            grpc_routes: None,
            axum_router: None,
            listeners: address_book.take_listeners::<FabricPort>(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.grpc_routes.is_none() && self.axum_router.is_none()
    }

    pub fn register_grpc_service<S>(
        &mut self,
        svc: S,
        file_descriptor_set: &'static [u8],
    ) -> &mut Self
    where
        S: tower::Service<
                Request<tonic::body::Body>,
                Response = http::Response<tonic::body::Body>,
                Error = Infallible,
            > + tonic::server::NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        let current_routes = self.grpc_routes.take().unwrap_or_default();
        self.grpc_descriptors.push(file_descriptor_set);
        debug!(svc = S::NAME, "Registering gRPC service to node-rpc-server");
        self.grpc_routes = Some(current_routes.add_service(svc));
        self
    }

    pub fn register_axum_routes(&mut self, routes: impl Into<axum::routing::Router>) {
        self.axum_router = Some(self.axum_router.take().unwrap_or_default().merge(routes));
    }

    pub async fn run(
        self,
        node_rpc_health: HealthStatus<NodeRpcStatus>,
    ) -> Result<(), anyhow::Error> {
        node_rpc_health.update(NodeRpcStatus::StartingUp);

        // Trace layer for HTTP requests
        let http_span_factory = tower_http::trace::DefaultMakeSpan::new()
            .include_headers(true)
            .level(tracing::Level::ERROR);

        // Prepare Axum router for HTTP requests
        let axum_router = self.axum_router.unwrap_or_default();

        // Prepare gRPC routes
        let mut grpc_routes = self.grpc_routes.unwrap_or_default();

        // Add reflection service to gRPC routes
        let mut reflection_service_builder = tonic_reflection::server::Builder::configure();
        for descriptor in self.grpc_descriptors {
            reflection_service_builder =
                reflection_service_builder.register_encoded_file_descriptor_set(descriptor);
        }
        grpc_routes = grpc_routes.add_service(reflection_service_builder.build_v1()?);

        // Convert gRPC routes to Axum router
        let grpc_router = grpc_routes.prepare().into_axum_router().layer(
            TraceLayer::new_for_grpc()
                .make_span_with(http_span_factory.clone())
                .on_failure(DefaultOnFailure::new().level(Level::DEBUG)),
        );

        // Merge HTTP and gRPC routers
        let combined_router = axum_router
            .layer(TraceLayer::new_for_http().make_span_with(http_span_factory))
            .merge(grpc_router)
            .fallback(handler_404);

        // Convert to hyper service using TowerToHyperService wrapper
        let service = TowerToHyperService::new(combined_router);

        node_rpc_health.update(NodeRpcStatus::Ready);

        run_hyper_server(self.listeners, service, || {
            node_rpc_health.update(NodeRpcStatus::Stopping)
        })
        .await?;

        Ok(())
    }
}

async fn handler_404() -> (http::StatusCode, &'static str) {
    (
        axum::http::StatusCode::NOT_FOUND,
        "Are you lost? Maybe visit https://restate.dev instead!",
    )
}
