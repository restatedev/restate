//! This module implements the Meta API endpoint.

mod endpoints;
mod error;
mod state;

use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use futures::FutureExt;
use hyper::Server;
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

    pub async fn run(self, meta_handle: MetaHandle, drain: drain::Watch) {
        let shared_state = Arc::new(state::RestEndpointState::new(meta_handle));

        // Setup the router
        let meta_api = Router::new()
            .route("/endpoint/discover", post(endpoints::discover_endpoint))
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
