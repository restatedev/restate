//! This module implements the Meta API endpoint.

mod error;
mod methods;
mod services;
mod state;

use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Router;
use futures::FutureExt;
use hyper::Server;
use service_metadata::{MethodDescriptorRegistry, ServiceEndpointRegistry};
use std::net::SocketAddr;
use std::sync::Arc;
use tower::ServiceBuilder;
use tracing::{debug, warn};

use crate::service::MetaHandle;

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
        S: ServiceEndpointRegistry + Send + Sync + 'static,
        M: MethodDescriptorRegistry + Send + Sync + 'static,
    >(
        self,
        meta_handle: MetaHandle,
        service_endpoint_registry: S,
        method_descriptor_registry: M,
        drain: drain::Watch,
    ) {
        let shared_state = Arc::new(state::RestEndpointState::new(
            meta_handle,
            service_endpoint_registry,
            method_descriptor_registry,
        ));

        // Setup the router
        let meta_api = Router::new()
            // deprecated url
            .route(
                "/endpoint/discover",
                post(services::discover_service_endpoint),
            )
            .route(
                "/services/discover",
                post(services::discover_service_endpoint),
            )
            .route("/services/", get(services::list_services))
            .route("/services/:service", get(services::get_service))
            .route(
                "/services/:service/methods/",
                get(methods::list_service_methods),
            )
            .route(
                "/services/:service/methods/:method",
                get(methods::get_service_method),
            )
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
        let server = Server::bind(&self.rest_addr).serve(meta_api.into_make_service());
        debug!(rest_addr = ?server.local_addr(), "Starting the meta component.");

        // Wait server graceful shutdown
        if let Err(e) = server
            .with_graceful_shutdown(drain.signaled().map(|_| ()))
            .await
        {
            warn!("Server is shutting down with error: {:?}", e);
        }
    }
}
