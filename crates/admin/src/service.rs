// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use axum::error_handling::HandleErrorLayer;
use http::{Request, Response, StatusCode};
use restate_ingestion_client::IngestionClient;
use restate_wal_protocol::Envelope;
use tower::ServiceBuilder;
use tower_http::classify::ServerErrorsFailureClass;
use tower_http::trace::TraceLayer;
use tracing::{Span, debug, info, info_span};

use restate_admin_rest_model::version::AdminApiVersion;
use restate_core::network::{TransportConnect, net_util};
use restate_core::{MetadataWriter, TaskCenter};
use restate_service_client::HttpClient;
use restate_service_protocol::discovery::ServiceDiscovery;
use restate_time_util::DurationExt;
use restate_types::config::AdminOptions;
use restate_types::invocation::client::InvocationClient;
use restate_types::live::LiveLoad;
use restate_types::net::address::AdminPort;
use restate_types::net::listener::Listeners;
use restate_types::schema::registry::SchemaRegistry;

use crate::rest_api::{MAX_ADMIN_API_VERSION, MIN_ADMIN_API_VERSION};
use crate::schema_registry_integration::{MetadataService, TelemetryClient};
use crate::{rest_api, state};

#[derive(Debug, thiserror::Error)]
#[error("could not create the service client: {0}")]
pub struct BuildError(#[from] restate_service_client::BuildError);

pub struct AdminService<Metadata, Discovery, Telemetry, Invocations, Transport> {
    listeners: Listeners<AdminPort>,
    ingestion_client: IngestionClient<Transport, Envelope>,
    schema_registry: SchemaRegistry<Metadata, Discovery, Telemetry>,
    invocation_client: Invocations,
    #[cfg(feature = "storage-query")]
    query_context: Option<restate_storage_query_datafusion::context::QueryContext>,
    #[cfg(feature = "metadata-api")]
    metadata_writer: MetadataWriter,
}

impl<Invocations, Transport>
    AdminService<MetadataService, ServiceDiscovery, TelemetryClient, Invocations, Transport>
where
    Invocations: InvocationClient + Send + Sync + Clone + 'static,
    Transport: TransportConnect,
{
    pub fn new(
        listeners: Listeners<AdminPort>,
        metadata_writer: MetadataWriter,
        ingestion_client: IngestionClient<Transport, Envelope>,
        invocation_client: Invocations,
        service_discovery: ServiceDiscovery,
        telemetry_http_client: Option<HttpClient>,
    ) -> Self {
        Self {
            listeners,
            ingestion_client,
            #[cfg(feature = "metadata-api")]
            metadata_writer: metadata_writer.clone(),
            schema_registry: SchemaRegistry::new(
                MetadataService(metadata_writer),
                service_discovery,
                TelemetryClient(telemetry_http_client),
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
            self.ingestion_client,
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

        // Add now the tracing layer, this makes sure we don't log ui requests
        let router = router.layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    info_span!(
                        target: "restate_admin::api",
                        "admin-api-request",
                        http.version = ?request.version(),
                        http.request.method = %request.method(),
                        url.path = request.uri().path(),
                        url.query = request.uri().query().unwrap_or_default(),
                        url.scheme = request.uri().scheme_str().unwrap_or("http")
                    )
                })
                // Just log on response
                .on_request(())
                .on_eos(())
                .on_body_chunk(())
                .on_response(
                    move |response: &Response<_>, latency: Duration, span: &Span| {
                        debug!(
                            name: "access-log",
                            target: "restate_admin::api",
                            parent: span,
                            { http.response.status_code = response.status().as_u16(), http.response.latency = %latency.friendly().to_seconds_span() },
                            "Replied"
                        )
                    },
                )
                .on_failure(
                    move |error: ServerErrorsFailureClass, latency: Duration, span: &Span| {
                        match error {
                            ServerErrorsFailureClass::StatusCode(_) => {
                                // No need to log it, on_response will log it already
                            }
                            ServerErrorsFailureClass::Error(error_string) => {
                                debug!(
                                    name: "access-log",
                                    target: "restate_admin::api",
                                    parent: span,
                                    { error.type = error_string, http.response.latency = %latency.friendly().to_seconds_span() },
                                    "Failed processing"
                                )
                            }
                        }
                    },
                ),
        );

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
            .merge(with_api_version_middleware(
                router.clone(),
                AdminApiVersion::Unknown,
            ))
            .nest("/v1", unsupported_api_version(AdminApiVersion::V1))
            .nest(
                "/v2",
                with_api_version_middleware(router.clone(), AdminApiVersion::V2),
            )
            .nest(
                "/v3",
                with_api_version_middleware(router, AdminApiVersion::V3),
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
            TaskCenter::with_current(|tc| opts.advertised_address(tc.address_book()))
        );

        net_util::run_hyper_server(self.listeners, service, || ())
            .await
            .map_err(Into::into)
    }
}

fn unsupported_api_version(version: AdminApiVersion) -> axum::Router {
    axum::Router::new().fallback((
        StatusCode::BAD_REQUEST,
        format!(
            "Unsupported Admin API version {:?}. This server supports versions between {:?} and {:?}.",
            version, MIN_ADMIN_API_VERSION, MAX_ADMIN_API_VERSION,
        ),
    ))
}

fn with_api_version_middleware(router: axum::Router, version: AdminApiVersion) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |mut request: axum::extract::Request, next: axum::middleware::Next| {
            request.extensions_mut().insert(version);
            next.run(request)
        },
    ))
}
