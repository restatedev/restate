//! This module implements the Meta API endpoint.

mod error;
mod methods;
mod services;
mod state;

use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::Router;
use codederror::CodedError;
use futures::FutureExt;
use hyper::Server;
use restate_service_metadata::{MethodDescriptorRegistry, ServiceEndpointRegistry};
use std::net::SocketAddr;
use std::sync::Arc;
use tower::ServiceBuilder;
use tracing::info;

use crate::service::MetaHandle;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum MetaRestServerError {
    #[error("error trying to bind meta's rest server to {address}: {source}")]
    #[code(restate_errors::META0004)]
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
        S: ServiceEndpointRegistry + Send + Sync + 'static,
        M: MethodDescriptorRegistry + Send + Sync + 'static,
    >(
        self,
        meta_handle: MetaHandle,
        service_endpoint_registry: S,
        method_descriptor_registry: M,
        drain: drain::Watch,
    ) -> Result<(), MetaRestServerError> {
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
