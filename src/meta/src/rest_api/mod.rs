//! This module implements the Meta API endpoint.

mod data;
mod handlers;

use crate::rest_api::handlers::RestEndpointState;
use crate::service::MetaHandle;
use axum::routing::post;
use axum::Router;
use futures::FutureExt;
use hyper::Server;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{debug, warn};

pub struct MetaRestEndpoint {
    rest_addr: SocketAddr,
}

impl MetaRestEndpoint {
    pub fn new(rest_addr: SocketAddr) -> Self {
        Self { rest_addr }
    }

    pub async fn run(self, meta_handle: MetaHandle, drain: drain::Watch) {
        let shared_state = Arc::new(RestEndpointState::new(meta_handle));

        // Setup the router
        let meta_api = Router::new()
            .route("/discover", post(handlers::register_endpoint))
            .with_state(shared_state);

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
