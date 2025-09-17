// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
mod services;
mod subscriptions;
mod version;

use axum_integration::put;
use okapi_operation::axum_integration::{delete, get, patch, post};
use okapi_operation::okapi::openapi3::{ExternalDocs, Tag};
use okapi_operation::*;
use restate_types::identifiers::PartitionKey;
use restate_types::invocation::client::InvocationClient;
use restate_types::schema::subscriptions::SubscriptionValidator;
use restate_wal_protocol::{Destination, Header, Source};

use crate::state::AdminServiceState;

pub use version::{MAX_ADMIN_API_VERSION, MIN_ADMIN_API_VERSION};

pub fn create_router<V, IC>(state: AdminServiceState<V, IC>) -> axum::Router<()>
where
    V: SubscriptionValidator + Send + Sync + Clone + 'static,
    IC: InvocationClient + Send + Sync + Clone + 'static,
{
    let mut router = axum_integration::Router::new()
        .route(
            "/deployments",
            get(openapi_handler!(deployments::list_deployments)),
        )
        .route(
            "/deployments",
            post(openapi_handler!(deployments::create_deployment)),
        )
        .route(
            "/deployments/{deployment}",
            get(openapi_handler!(deployments::get_deployment)),
        )
        .route(
            "/deployments/{deployment}",
            delete(openapi_handler!(deployments::delete_deployment)),
        )
        .route(
            "/deployments/{deployment}",
            put(openapi_handler!(deployments::update_deployment)),
        )
        .route("/services", get(openapi_handler!(services::list_services)))
        .route(
            "/services/{service}",
            get(openapi_handler!(services::get_service)),
        )
        .route(
            "/services/{service}/openapi",
            get(openapi_handler!(services::get_service_openapi)),
        )
        .route(
            "/services/{service}",
            patch(openapi_handler!(services::modify_service)),
        )
        .route(
            "/services/{service}/state",
            post(openapi_handler!(services::modify_service_state)),
        )
        .route(
            "/services/{service}/handlers",
            get(openapi_handler!(handlers::list_service_handlers)),
        )
        .route(
            "/services/{service}/handlers/{handler}",
            get(openapi_handler!(handlers::get_service_handler)),
        )
        .route(
            "/invocations/{invocation_id}",
            delete(openapi_handler!(invocations::delete_invocation)),
        )
        .route(
            "/invocations/{invocation_id}/kill",
            patch(openapi_handler!(invocations::kill_invocation)),
        )
        .route(
            "/invocations/{invocation_id}/cancel",
            patch(openapi_handler!(invocations::cancel_invocation)),
        )
        .route(
            "/invocations/{invocation_id}/purge",
            patch(openapi_handler!(invocations::purge_invocation)),
        )
        .route(
            "/invocations/{invocation_id}/purge-journal",
            patch(openapi_handler!(invocations::purge_journal)),
        )
        .route(
            "/invocations/{invocation_id}/restart-as-new",
            patch(openapi_handler!(invocations::restart_as_new_invocation)),
        )
        .route(
            "/invocations/{invocation_id}/resume",
            patch(openapi_handler!(invocations::resume_invocation)),
        )
        .route(
            "/subscriptions",
            post(openapi_handler!(subscriptions::create_subscription)),
        )
        .route(
            "/subscriptions",
            get(openapi_handler!(subscriptions::list_subscriptions)),
        )
        .route(
            "/subscriptions/{subscription}",
            get(openapi_handler!(subscriptions::get_subscription)),
        )
        .route(
            "/subscriptions/{subscription}",
            delete(openapi_handler!(subscriptions::delete_subscription)),
        )
        .route("/health", get(openapi_handler!(health::health)))
        .route("/version", get(openapi_handler!(version::version)))
        .route(
            "/cluster-health",
            get(openapi_handler!(cluster_health::cluster_health)),
        );

    // Add some additional OpenAPI metadata
    router.openapi_builder_template_mut()
        .description("This API exposes the admin operations of a Restate cluster, such as registering new service deployments, interacting with running invocations, register Kafka subscriptions, retrieve service metadata. For an overview, check out the [Operate documentation](https://docs.restate.dev/operate/). If you're looking for how to call your services, check out the [Ingress HTTP API](https://docs.restate.dev/invoke/http) instead.")
        .external_docs(ExternalDocs {
            url: "https://docs.restate.dev/operate/".to_string(),
            ..Default::default()
        })
        .tag(Tag {
            name: "deployment".to_string(),
            description: Some("Service Deployment management".to_string()),
            ..Default::default()
        })
        .tag(Tag {
            name: "invocation".to_string(),
            description: Some("Invocation management".to_string()),
            external_docs: Some(ExternalDocs {
                url: "https://docs.restate.dev/operate/invocation".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .tag(Tag {
            name: "subscription".to_string(),
            description: Some("Subscription management".to_string()),
            external_docs: Some(ExternalDocs {
                url: "https://docs.restate.dev/operate/invocation#managing-kafka-subscriptions".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .tag(Tag {
            name: "service".to_string(),
            description: Some("Service management".to_string()),
            ..Default::default()
        })
        .tag(Tag {
            name: "service_handler".to_string(),
            description: Some("Service handlers metadata".to_string()),
            ..Default::default()
        })
        .tag(Tag {
            name: "cluster_health".to_string(),
            description: Some("Cluster health".to_string()),
            ..Default::default()
        })
        .tag(Tag {
            name: "health".to_string(),
            description: Some("Admin API health".to_string()),
            ..Default::default()
        })
        .tag(Tag {
            name: "version".to_string(),
            description: Some("API Version".to_string()),
            ..Default::default()
        });

    // Finish router
    router
        .finish_openapi("/openapi", "Admin API", env!("CARGO_PKG_VERSION"))
        .expect("Error when building the OpenAPI specification")
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
