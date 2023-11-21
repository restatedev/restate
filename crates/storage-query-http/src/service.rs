// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{query, state};
use axum::error_handling::HandleErrorLayer;
use axum::http::StatusCode;
use codederror::CodedError;

use futures::FutureExt;
use hyper::Server;
use okapi_operation::axum_integration::post;
use okapi_operation::*;
use restate_storage_query_datafusion::context::QueryContext;
use std::net::SocketAddr;
use std::sync::Arc;
use tower::ServiceBuilder;
use tracing::info;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error(
        "failed binding to address '{address}' specified in 'meta.storage_query_http.bind_address'"
    )]
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

pub struct HTTPQueryService {
    http_address: SocketAddr,
    concurrency_limit: usize,
    query_context: QueryContext,
}

impl HTTPQueryService {
    pub fn new(
        http_address: SocketAddr,
        concurrency_limit: usize,
        query_context: QueryContext,
    ) -> Self {
        Self {
            http_address,
            concurrency_limit,
            query_context,
        }
    }

    pub async fn run(self, drain: drain::Watch) -> Result<(), Error> {
        let shared_state = Arc::new(state::EndpointState::new(self.query_context));

        // Setup the router
        let storage_api = axum_integration::Router::new()
            .route("/api/query", post(openapi_handler!(query::query)))
            .route_openapi_specification(
                "/api/openapi",
                OpenApiBuilder::new("Storage Query API", env!("CARGO_PKG_VERSION")),
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
        let server = Server::try_bind(&self.http_address)
            .map_err(|err| Error::Binding {
                address: self.http_address,
                source: err,
            })?
            .serve(storage_api.into_make_service());

        info!(
            net.host.addr = %server.local_addr().ip(),
            net.host.port = %server.local_addr().port(),
            "Storage HTTP API listening"
        );

        // Wait server graceful shutdown
        server
            .with_graceful_shutdown(drain.signaled().map(|_| ()))
            .await
            .map_err(Error::Running)
    }
}
