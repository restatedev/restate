//! This module implements the Meta API endpoint.

mod error;
mod invocations;
mod methods;
mod services;
mod state;

use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use codederror::CodedError;
use futures::FutureExt;
use hyper::Server;
use okapi_operation::axum_integration::{delete, get, post};
use okapi_operation::*;
use restate_service_metadata::{MethodDescriptorRegistry, ServiceEndpointRegistry};
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
        let meta_api = axum_integration::Router::new()
            // deprecated url
            .route(
                "/endpoint/discover",
                post(openapi_handler!(services::discover_service_endpoint)),
            )
            .route(
                "/services/discover",
                post(openapi_handler!(services::discover_service_endpoint)),
            )
            .route("/services/", get(openapi_handler!(services::list_services)))
            .route(
                "/services/:service",
                get(openapi_handler!(services::get_service)),
            )
            .route(
                "/services/:service/methods/",
                get(openapi_handler!(methods::list_service_methods)),
            )
            .route(
                "/services/:service/methods/:method",
                get(openapi_handler!(methods::get_service_method)),
            )
            .route(
                "/invocations",
                delete(openapi_handler!(invocations::cancel_invocation)),
            )
            .route_openapi_specification(
                "/openapi",
                OpenApiBuilder::new("Meta REST Operational API", env!("CARGO_PKG_VERSION")),
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
