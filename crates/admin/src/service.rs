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
use http::StatusCode;
use tonic::transport::Channel;
use tower::ServiceBuilder;
use tracing::info;

use restate_core::cancellation_watcher;
use restate_meta::{FileMetaReader, MetaHandle};
use restate_node_services::node::node_svc_client::NodeSvcClient;
use restate_schema_impl::Schemas;

use crate::{rest_api, state, storage_query};
use crate::{Error, Options};

#[derive(Debug)]
pub struct AdminService {
    opts: Options,
    schemas: Schemas,
    meta_handle: MetaHandle,
    schema_reader: FileMetaReader,
}

impl AdminService {
    pub fn new(
        opts: Options,
        schemas: Schemas,
        meta_handle: MetaHandle,
        schema_reader: FileMetaReader,
    ) -> Self {
        Self {
            opts,
            schemas,
            meta_handle,
            schema_reader,
        }
    }

    pub async fn run(
        self,
        worker_handle: impl restate_worker_api::Handle + Clone + Send + Sync + 'static,
        node_svc_client: NodeSvcClient<Channel>,
    ) -> anyhow::Result<()> {
        let rest_state = state::AdminServiceState::new(
            self.meta_handle,
            self.schemas,
            worker_handle,
            node_svc_client.clone(),
            self.schema_reader,
        );

        let query_state = Arc::new(state::QueryServiceState { node_svc_client });
        let router = axum::Router::new().merge(storage_query::create_router(query_state));

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
        Ok(server
            .with_graceful_shutdown(cancellation_watcher())
            .await
            .map_err(Error::Running)?)
    }
}
