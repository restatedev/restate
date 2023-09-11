// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements the Meta API endpoint.

mod endpoints;
mod error;
mod health;
mod invocations;
mod methods;
mod services;
mod state;
mod subscriptions;

use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use codederror::CodedError;
use futures::FutureExt;
use hyper::Server;
use okapi_operation::axum_integration::{delete, get, patch, post};
use okapi_operation::*;
use restate_schema_api::endpoint::EndpointMetadataResolver;
use restate_schema_api::service::ServiceMetadataResolver;
use restate_schema_api::subscription::SubscriptionResolver;
use std::net::SocketAddr;
use std::sync::Arc;
use tower::ServiceBuilder;
use tracing::info;

use crate::service::MetaHandle;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum MetaRestServerError {
    #[error("failed binding to address '{address}' specified in 'meta.rest_address'")]
    #[code(restate_errors::RT0004)]
    Binding {
        address: SocketAddr,
        #[source]
        source: hyper::Error,
    },
    #[error("error while running meta's rest server: {0}")]
    #[code(unknown)]
    Running(hyper::Error),
}

pub struct MetaRestEndpoint {
    rest_addr: SocketAddr,
    concurrency_limit: usize,
}

impl MetaRestEndpoint {
    pub fn new(rest_addr: SocketAddr, concurrency_limit: usize) -> Self {
        Self {
            rest_addr,
            concurrency_limit,
        }
    }

    pub async fn run<
        S: ServiceMetadataResolver
            + EndpointMetadataResolver
            + SubscriptionResolver
            + Send
            + Sync
            + 'static,
        W: restate_worker_api::Handle + Send + Sync + 'static,
    >(
        self,
        meta_handle: MetaHandle,
        schemas: S,
        worker_handle: W,
        drain: drain::Watch,
    ) -> Result<(), MetaRestServerError> {
        let shared_state = Arc::new(state::RestEndpointState::new(
            meta_handle,
            schemas,
            worker_handle,
        ));

        // Setup the router
        let meta_api = axum_integration::Router::new()
            .route(
                "/endpoints",
                get(openapi_handler!(endpoints::list_service_endpoints)),
            )
            .route(
                "/endpoints",
                post(openapi_handler!(endpoints::create_service_endpoint)),
            )
            .route(
                "/endpoints/:endpoint",
                get(openapi_handler!(endpoints::get_service_endpoint)),
            )
            .route(
                "/endpoints/:endpoint",
                delete(openapi_handler!(endpoints::delete_service_endpoint)),
            )
            .route("/services", get(openapi_handler!(services::list_services)))
            .route(
                "/services/:service",
                get(openapi_handler!(services::get_service)),
            )
            .route(
                "/services/:service",
                patch(openapi_handler!(services::modify_service)),
            )
            .route(
                "/services/:service/methods",
                get(openapi_handler!(methods::list_service_methods)),
            )
            .route(
                "/services/:service/methods/:method",
                get(openapi_handler!(methods::get_service_method)),
            )
            .route(
                "/invocations/:invocation_id",
                delete(openapi_handler!(invocations::cancel_invocation)),
            )
            .route(
                "/subscriptions",
                post(openapi_handler!(subscriptions::create_subscription)),
            )
            .route(
                "/subscriptions/:subscription",
                get(openapi_handler!(subscriptions::get_subscription)),
            )
            .route(
                "/subscriptions/:subscription",
                delete(openapi_handler!(subscriptions::delete_subscription)),
            )
            .route("/health", get(openapi_handler!(health::health)))
            .route_openapi_specification(
                "/openapi",
                OpenApiBuilder::new("Admin API", env!("CARGO_PKG_VERSION")),
            )
            .expect("Error when building the OpenAPI specification")
            .with_state(shared_state)
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_| async {
                        StatusCode::TOO_MANY_REQUESTS
                    }))
                    .layer(tower::load_shed::LoadShedLayer::new())
                    .layer(tower::limit::GlobalConcurrencyLimitLayer::new(
                        self.concurrency_limit,
                    )),
            );

        // Start the server
        let server = Server::try_bind(&self.rest_addr)
            .map_err(|err| MetaRestServerError::Binding {
                address: self.rest_addr,
                source: err,
            })?
            .serve(meta_api.into_make_service());

        info!(
            net.host.addr = %server.local_addr().ip(),
            net.host.port = %server.local_addr().port(),
            "Meta Operational REST API listening"
        );

        // Wait server graceful shutdown
        server
            .with_graceful_shutdown(drain.signaled().map(|_| ()))
            .await
            .map_err(MetaRestServerError::Running)
    }
}
