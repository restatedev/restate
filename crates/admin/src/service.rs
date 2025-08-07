// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::error_handling::HandleErrorLayer;
use http::StatusCode;
use restate_admin_rest_model::version::AdminApiVersion;
use restate_bifrost::Bifrost;
use restate_types::config::AdminOptions;
use restate_types::live::LiveLoad;
use tower::ServiceBuilder;

use crate::schema_registry::SchemaRegistry;
use crate::{rest_api, state};
use restate_core::MetadataWriter;
use restate_core::network::net_util;
use restate_service_client::HttpClient;
use restate_service_protocol::discovery::ServiceDiscovery;
use restate_types::invocation::client::InvocationClient;
use restate_types::net::BindAddress;
use restate_types::schema::subscriptions::SubscriptionValidator;
use tracing::info;

#[derive(Debug, thiserror::Error)]
#[error("could not create the service client: {0}")]
pub struct BuildError(#[from] restate_service_client::BuildError);

pub struct AdminService<V, IC> {
    bifrost: Bifrost,
    schema_registry: SchemaRegistry<V>,
    invocation_client: IC,
    #[cfg(feature = "storage-query")]
    query_context: Option<restate_storage_query_datafusion::context::QueryContext>,
    #[cfg(feature = "metadata-api")]
    metadata_writer: MetadataWriter,
}

impl<V, IC> AdminService<V, IC>
where
    V: SubscriptionValidator + Send + Sync + Clone + 'static,
    IC: InvocationClient + Send + Sync + Clone + 'static,
{
    pub fn new(
        metadata_writer: MetadataWriter,
        bifrost: Bifrost,
        invocation_client: IC,
        subscription_validator: V,
        service_discovery: ServiceDiscovery,
        telemetry_http_client: Option<HttpClient>,
    ) -> Self {
        Self {
            bifrost,
            #[cfg(feature = "metadata-api")]
            metadata_writer: metadata_writer.clone(),
            schema_registry: SchemaRegistry::new(
                metadata_writer,
                service_discovery,
                telemetry_http_client,
                subscription_validator,
            ),
            invocation_client,
            #[cfg(feature = "storage-query")]
            query_context: None,
        }
    }

    #[cfg(feature = "storage-query")]
    pub fn with_query_context(
        self,
        query_context: restate_storage_query_datafusion::context::QueryContext,
    ) -> Self {
        Self {
            query_context: Some(query_context),
            ..self
        }
    }

    pub async fn run(
        self,
        mut updateable_config: impl LiveLoad<Live = AdminOptions>,
    ) -> anyhow::Result<()> {
        let opts = updateable_config.live_load();

        let rest_state = state::AdminServiceState::new(
            self.schema_registry,
            self.invocation_client,
            self.bifrost,
        );

        let router = axum::Router::new();

        #[cfg(feature = "storage-query")]
        let router = if let Some(query_context) = self.query_context {
            router.merge(crate::storage_query::router(query_context))
        } else {
            router
        };

        #[cfg(feature = "metadata-api")]
        let router = router.merge(crate::metadata_api::router(
            self.metadata_writer.raw_metadata_store_client(),
        ));

        // Merge Web UI router
        #[cfg(feature = "serve-web-ui")]
        let router = if !opts.disable_web_ui {
            router.merge(crate::web_ui::web_ui_router())
        } else {
            router
        };

        // Merge meta API router
        let router = router.merge(rest_api::create_router(rest_state));

        let router = axum::Router::new()
            .merge(with_unknown_api_version_middleware(router.clone()))
            .nest(
                "/v1",
                with_api_version_middleware(router.clone(), AdminApiVersion::V1),
            )
            .nest(
                "/v2",
                with_api_version_middleware(router, AdminApiVersion::V2),
            )
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

        let service = hyper_util::service::TowerToHyperService::new(router.into_service());

        info!(
            "Admin API starting on: {}",
            opts.advertised_admin_endpoint.as_ref().expect("is set")
        );

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

fn with_unknown_api_version_middleware(router: axum::Router) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |mut request: axum::extract::Request, next: axum::middleware::Next| {
    let is_cli = matches!(request.headers().get(http::header::USER_AGENT), Some(value) if value.as_bytes().starts_with(b"restate-cli"));

    if is_cli {
        // If the CLI is using an unversioned API url, it must be be pre 1.2, so api version v1
        request.extensions_mut().insert(AdminApiVersion::V1);
    } else {
        request.extensions_mut().insert(AdminApiVersion::Unknown);
    }

    next.run(request)

    }))
}

fn with_api_version_middleware(router: axum::Router, version: AdminApiVersion) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |mut request: axum::extract::Request, next: axum::middleware::Next| {
            request.extensions_mut().insert(version);
            next.run(request)
        },
    ))
}
