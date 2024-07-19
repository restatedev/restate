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
use restate_bifrost::Bifrost;
use restate_types::config::AdminOptions;
use restate_types::live::LiveLoad;
use tonic::transport::Channel;
use tower::ServiceBuilder;
use tracing::info;

use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::protobuf::node_svc::node_svc_client::NodeSvcClient;
use restate_core::{cancellation_token, task_center, MetadataWriter};
use restate_service_protocol::discovery::ServiceDiscovery;
use restate_types::schema::subscriptions::SubscriptionValidator;

use crate::schema_registry::SchemaRegistry;
use crate::Error;
use crate::{rest_api, state, storage_query};

#[derive(Debug, thiserror::Error)]
#[error("could not create the service client: {0}")]
pub struct BuildError(#[from] restate_service_client::BuildError);

pub struct AdminService<V> {
    schema_registry: SchemaRegistry<V>,
}

impl<V> AdminService<V>
where
    V: SubscriptionValidator + Send + Sync + Clone + 'static,
{
    pub fn new(
        metadata_writer: MetadataWriter,
        metadata_store_client: MetadataStoreClient,
        subscription_validator: V,
        service_discovery: ServiceDiscovery,
    ) -> Self {
        Self {
            schema_registry: SchemaRegistry::new(
                metadata_store_client,
                metadata_writer,
                service_discovery,
                subscription_validator,
            ),
        }
    }

    pub async fn run(
        self,
        mut updateable_config: impl LiveLoad<AdminOptions> + Send + 'static,
        node_svc_client: NodeSvcClient<Channel>,
        bifrost: Bifrost,
    ) -> anyhow::Result<()> {
        let opts = updateable_config.live_load();

        let rest_state =
            state::AdminServiceState::new(self.schema_registry, bifrost, task_center());

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
                        opts.concurrent_api_requests_limit(),
                    )),
            );

        // run our app with hyper
        let listener = tokio::net::TcpListener::bind(&opts.bind_address)
            .await
            .map_err(|err| Error::Binding {
                address: opts.bind_address,
                source: Box::new(err),
            })?;
        if let Ok(local_addr) = listener.local_addr() {
            info!(
                net.host.addr = %local_addr.ip(),
                net.host.port = %local_addr.port(),
                "Admin API listening"
            );
        } else {
            info!("Admin API listening");
        }
        let cancellation_token = cancellation_token();
        Ok(axum::serve(listener, router)
            .with_graceful_shutdown(async move { cancellation_token.cancelled().await })
            .await
            .map_err(|e| Error::Running(Box::new(e)))?)
    }
}
