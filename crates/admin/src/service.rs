// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use axum::error_handling::HandleErrorLayer;
use futures::FutureExt;
use http::StatusCode;
use tonic::transport::Channel;
use tower::ServiceBuilder;

use restate_meta::MetaHandle;
use restate_node_services::worker::worker_svc_client::WorkerSvcClient;
use restate_schema_impl::Schemas;
use tracing::info;

use crate::{rest_api, state, storage_query};
use crate::{Error, Options};

#[derive(Debug)]
pub struct AdminService {
    opts: Options,
    schemas: Schemas,
    meta_handle: MetaHandle,
}

impl AdminService {
    pub fn new(opts: Options, schemas: Schemas, meta_handle: MetaHandle) -> Self {
        Self {
            opts,
            schemas,
            meta_handle,
        }
    }

    pub async fn run(
        self,
        drain: drain::Watch,
        worker_handle: impl restate_worker_api::Handle + Clone + Send + Sync + 'static,
        worker_svc_client: Option<WorkerSvcClient<Channel>>,
    ) -> Result<(), Error> {
        let rest_state =
            state::AdminServiceState::new(self.meta_handle, self.schemas, worker_handle);

        let router = axum::Router::new();

        // Stitch query http endpoint if enabled
        let router = if let Some(worker_svc_client) = worker_svc_client {
            let query_state = Arc::new(state::QueryServiceState { worker_svc_client });
            // Merge storage query router
            router.merge(storage_query::create_router(query_state))
        } else {
            router
        };

        let router = router
            // Merge meta API router
            .merge(rest_api::create_router(rest_state))
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_| async {
                        StatusCode::TOO_MANY_REQUESTS
                    }))
                    .layer(tower::load_shed::LoadShedLayer::new())
                    .layer(tower::limit::GlobalConcurrencyLimitLayer::new(
                        self.opts.concurrency_limit,
                    )),
            );

        // Bind and serve
        let server = hyper::Server::try_bind(&self.opts.bind_address)
            .map_err(|err| Error::Binding {
                address: self.opts.bind_address,
                source: err,
            })?
            .serve(router.into_make_service());

        info!(
            net.host.addr = %server.local_addr().ip(),
            net.host.port = %server.local_addr().port(),
            "Admin API listening"
        );

        // Wait server graceful shutdown
        server
            .with_graceful_shutdown(drain.signaled().map(|_| ()))
            .await
            .map_err(Error::Running)
    }
}
