// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements the Meta API endpoint.

mod cluster_health;
mod deployments;
mod error;
mod handlers;
mod health;
mod invocations;
mod kafka_clusters;
mod query;
mod services;
mod subscriptions;
mod version;

use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

use restate_core::network::TransportConnect;
use restate_types::identifiers::PartitionKey;
use restate_types::invocation::client::InvocationClient;
use restate_types::schema::registry::{DiscoveryClient, MetadataService, TelemetryClient};
use restate_wal_protocol::{Destination, Header, Source};

use crate::state::AdminServiceState;

pub use version::{MAX_ADMIN_API_VERSION, MIN_ADMIN_API_VERSION};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Admin API",
        version = env!("CARGO_PKG_VERSION"),
        description = "This API exposes the admin operations of a Restate cluster, such as registering new service deployments, interacting with running invocations, register Kafka subscriptions, retrieve service metadata. For an overview, check out the [Operate documentation](https://docs.restate.dev/operate/). If you're looking for how to call your services, check out the [Ingress HTTP API](https://docs.restate.dev/invoke/http) instead.",
        license(
            name = "MIT",
            url = "https://opensource.org/license/mit"
        ),
    ),
    external_docs(url = "https://docs.restate.dev/operate/", description = "Restate operations documentation"),
    tags(
        (name = "deployment", description = "Service Deployment management"),
        (name = "invocation", description = "Invocation management",
         external_docs(url = "https://docs.restate.dev/operate/invocation", description = "Invocations documentation")),
        (name = "subscription", description = "Subscription management",
         external_docs(url = "https://docs.restate.dev/operate/invocation#managing-kafka-subscriptions", description = "Kafka subscriptions documentation")),
        (name = "kafka_cluster", description = "Kafka cluster management"),
        (name = "service", description = "Service management"),
        (name = "service_handler", description = "Service handlers metadata"),
        (name = "cluster_health", description = "Cluster health"),
        (name = "health", description = "Admin API health"),
        (name = "version", description = "API Version"),
        (name = "introspection", description = "System introspection"),
    ),
    components(responses(
        error::meta_api_error::BadRequest,
        error::meta_api_error::NotFound,
        error::meta_api_error::MethodNotAllowed,
        error::meta_api_error::Conflict,
        error::meta_api_error::InternalServerError))
)]
struct AdminApiDoc;

pub fn create_router<Metadata, Discovery, Telemetry, Invocations, Transport>(
    state: AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>,
) -> axum::Router<()>
where
    Metadata: MetadataService + Send + Sync + Clone + 'static,
    Discovery: DiscoveryClient + Send + Sync + Clone + 'static,
    Telemetry: TelemetryClient + Send + Sync + Clone + 'static,
    Invocations: InvocationClient + Send + Sync + Clone + 'static,
    Transport: TransportConnect,
{
    let router = {
        #[allow(deprecated)]
        OpenApiRouter::with_openapi(AdminApiDoc::openapi())
            .routes(routes!(health::health))
            .routes(routes!(version::version))
            .routes(routes!(cluster_health::cluster_health))
            // Deployment endpoints
            .routes(routes!(deployments::list_deployments))
            .routes(routes!(deployments::create_deployment))
            .routes(routes!(deployments::get_deployment))
            .routes(routes!(deployments::delete_deployment))
            .routes(routes!(deployments::update_deployment))
            // Service endpoints
            .routes(routes!(services::list_services))
            .routes(routes!(services::get_service))
            .routes(routes!(services::get_service_openapi))
            .routes(routes!(services::modify_service))
            .routes(routes!(services::modify_service_state))
            // Handler endpoints
            .routes(routes!(handlers::list_service_handlers))
            .routes(routes!(handlers::get_service_handler))
            // Invocation endpoints
            .routes(routes!(invocations::delete_invocation))
            .routes(routes!(invocations::kill_invocation))
            .routes(routes!(invocations::cancel_invocation))
            .routes(routes!(invocations::purge_invocation))
            .routes(routes!(invocations::purge_journal))
            .routes(routes!(invocations::restart_as_new_invocation))
            .routes(routes!(invocations::resume_invocation))
            .routes(routes!(invocations::pause_invocation))
            // Subscription endpoints
            .routes(routes!(subscriptions::create_subscription))
            .routes(routes!(subscriptions::list_subscriptions))
            .routes(routes!(subscriptions::get_subscription))
            .routes(routes!(subscriptions::delete_subscription))
            // Kafka cluster endpoints
            .routes(routes!(kafka_clusters::create_kafka_cluster))
            .routes(routes!(kafka_clusters::list_kafka_clusters))
            .routes(routes!(kafka_clusters::get_kafka_cluster))
            .routes(routes!(kafka_clusters::update_kafka_cluster))
            .routes(routes!(kafka_clusters::delete_kafka_cluster))
            // Query endpoint
            .routes(routes!(query::query))
    };

    let (router, api) = router.split_for_parts();

    // The PUT route is deprecated and intentionally not documented in OpenAPI
    // Only PATCH has #[utoipa::path], PUT is kept for backward compatibility only
    axum::Router::new()
        .merge(router)
        .route(
            "/deployments/{deployment}",
            axum::routing::put(deployments::update_deployment),
        )
        // Internal batch operation routes (for UI only, not documented in OpenAPI)
        .route(
            "/internal/invocations_batch_operations/kill",
            axum::routing::post(invocations::batch_kill_invocations),
        )
        .route(
            "/internal/invocations_batch_operations/cancel",
            axum::routing::post(invocations::batch_cancel_invocations),
        )
        .route(
            "/internal/invocations_batch_operations/purge",
            axum::routing::post(invocations::batch_purge_invocations),
        )
        .route(
            "/internal/invocations_batch_operations/purge-journal",
            axum::routing::post(invocations::batch_purge_journal),
        )
        .route(
            "/internal/invocations_batch_operations/restart-as-new",
            axum::routing::post(invocations::batch_restart_as_new_invocations),
        )
        .route(
            "/internal/invocations_batch_operations/resume",
            axum::routing::post(invocations::batch_resume_invocations),
        )
        .route(
            "/internal/invocations_batch_operations/pause",
            axum::routing::post(invocations::batch_pause_invocations),
        )
        .route(
            "/openapi",
            axum::routing::get(|| async move { axum::Json(api) }),
        )
        .with_state(state)
}

fn create_envelope_header(partition_key: PartitionKey) -> Header {
    Header {
        source: Source::ControlPlane {},
        dest: Destination::Processor {
            partition_key,
            dedup: None,
        },
    }
}
