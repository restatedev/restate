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
use restate_meta::{FileMetaReader, MetaReader};
use restate_node_services::node_svc::node_svc_client::NodeSvcClient;
use restate_node_services::node_svc::UpdateSchemaRequest;
use restate_types::identifiers::PartitionKey;
use restate_wal_protocol::{Destination, Header, Source};
use tonic::transport::Channel;
use tracing::debug;

use crate::state::AdminServiceState;

pub fn create_router(state: AdminServiceState) -> axum::Router<()> {
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
            "/components/:service/handlers/:handler",
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

/// Notifies the node about schema changes. This method is best-effort and will not fail if the node
/// could not be reached or the schema changes cannot serialized.
async fn notify_node_about_schema_changes(
    schema_reader: &FileMetaReader,
    mut node_svc_client: NodeSvcClient<Channel>,
) {
    let schema_updates = schema_reader
        .read()
        .await
        .map_err(anyhow::Error::from)
        .and_then(|schema_updates| {
            bincode::serde::encode_to_vec(schema_updates, bincode::config::standard())
                .map_err(anyhow::Error::from)
        });

    if let Err(err) = schema_updates {
        debug!("Failed serializing schema changes for notifying node about schema changes: {err}");
        return;
    }

    let schema_updates = schema_updates.unwrap();

    let result = node_svc_client
        .update_schemas(UpdateSchemaRequest {
            schema_bin: schema_updates.into(),
        })
        .await;

    if let Err(err) = result {
        debug!("Failed notifying node about schema changes: {err}");
    }
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
