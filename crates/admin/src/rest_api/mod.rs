// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module implements the Meta API endpoint.

mod components;
mod deployments;
mod error;
mod handlers;
mod health;
mod invocations;
mod subscriptions;

use okapi_operation::axum_integration::{delete, get, patch, post};
use okapi_operation::*;
use restate_schema_api::subscription::SubscriptionValidator;
use restate_types::identifiers::PartitionKey;
use restate_wal_protocol::{Destination, Header, Source};

use crate::state::AdminServiceState;

pub fn create_router<V>(state: AdminServiceState<V>) -> axum::Router<()>
where
    V: SubscriptionValidator + Send + Sync + Clone + 'static,
{
    // Setup the router
    axum_integration::Router::new()
        .route(
            "/deployments",
            get(openapi_handler!(deployments::list_deployments)),
        )
        .route(
            "/deployments",
            post(openapi_handler!(deployments::create_deployment)),
        )
        .route(
            "/deployments/:deployment",
            get(openapi_handler!(deployments::get_deployment)),
        )
        .route(
            "/deployments/:deployment",
            delete(openapi_handler!(deployments::delete_deployment)),
        )
        .route(
            "/components",
            get(openapi_handler!(components::list_components)),
        )
        .route(
            "/components/:component",
            get(openapi_handler!(components::get_component)),
        )
        .route(
            "/components/:component",
            patch(openapi_handler!(components::modify_component)),
        )
        .route(
            "/components/:component/state",
            post(openapi_handler!(components::modify_component_state)),
        )
        .route(
            "/components/:component/handlers",
            get(openapi_handler!(handlers::list_component_handlers)),
        )
        .route(
            "/components/:component/handlers/:handler",
            get(openapi_handler!(handlers::get_component_handler)),
        )
        .route(
            "/invocations/:invocation_id",
            delete(openapi_handler!(invocations::delete_invocation)),
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
            "/subscriptions/:subscription",
            get(openapi_handler!(subscriptions::get_subscription)),
        )
        .route(
            "/subscriptions/:subscription",
            delete(openapi_handler!(subscriptions::delete_subscription)),
        )
        .route("/health", get(openapi_handler!(health::health)))
        .route_openapi_specification(
            "/openapi",
            OpenApiBuilder::new("Admin API", env!("CARGO_PKG_VERSION")),
        )
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
