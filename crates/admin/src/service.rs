// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use tower::ServiceBuilder;

use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::net_util;
use restate_core::MetadataWriter;
use restate_service_protocol::discovery::ServiceDiscovery;
use restate_storage_query_datafusion::context::QueryContext;
use restate_types::net::BindAddress;
use restate_types::schema::subscriptions::SubscriptionValidator;

use crate::schema_registry::SchemaRegistry;
use crate::{rest_api, state, storage_query};

#[derive(Debug, thiserror::Error)]
#[error("could not create the service client: {0}")]
pub struct BuildError(#[from] restate_service_client::BuildError);

pub struct AdminService<V> {
    bifrost: Bifrost,
    schema_registry: SchemaRegistry<V>,
    query_context: Option<QueryContext>,
}

impl<V> AdminService<V>
where
    V: SubscriptionValidator + Send + Sync + Clone + 'static,
{
    pub fn new(
        metadata_writer: MetadataWriter,
        metadata_store_client: MetadataStoreClient,
        bifrost: Bifrost,
        subscription_validator: V,
        service_discovery: ServiceDiscovery,
        experimental_feature_kafka_ingress_next: bool,
        query_context: Option<QueryContext>,
    ) -> Self {
        Self {
            bifrost,
            schema_registry: SchemaRegistry::new(
                metadata_store_client,
                metadata_writer,
                service_discovery,
                subscription_validator,
                experimental_feature_kafka_ingress_next,
            ),
            query_context,
        }
    }

    pub async fn run(
        self,
        mut updateable_config: impl LiveLoad<AdminOptions> + Send + 'static,
    ) -> anyhow::Result<()> {
        let opts = updateable_config.live_load();

        let rest_state = state::AdminServiceState::new(self.schema_registry, self.bifrost);

        let router = self
            .query_context
            .map(|query_context| {
                let query_state = Arc::new(state::QueryServiceState { query_context });

                axum::Router::new().merge(storage_query::create_router(query_state))
            })
            .unwrap_or_default();

        // Merge Web UI router
        #[cfg(feature = "serve-web-ui")]
        let router = router.merge(crate::web_ui::web_ui_router());

        // Merge meta API router
        let router = router.merge(rest_api::create_router(rest_state)).layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|_| async {
                    StatusCode::TOO_MANY_REQUESTS
                }))
                .layer(tower::load_shed::LoadShedLayer::new())
                .layer(tower::limit::GlobalConcurrencyLimitLayer::new(
                    opts.concurrent_api_requests_limit(),
                )),
        );

        let service = hyper_util::service::TowerToHyperService::new(router.into_service());

        net_util::run_hyper_server(
            &BindAddress::Socket(opts.bind_address),
            service,
            "admin-api-server",
            || (),
            || (),
        )
        .await
        .map_err(Into::into)
    }
}
