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

mod deployments;
mod error;
mod health;
mod invocations;
mod methods;
mod services;
mod subscriptions;

use crate::rest_api::error::MetaApiError;
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
            "/deployments/:deployment/descriptors",
            get(openapi_handler!(deployments::get_deployment_descriptors)),
        )
        .route(
            "/deployments/:deployment",
            delete(openapi_handler!(deployments::delete_deployment)),
        )
        .route("/services", get(openapi_handler!(services::list_services)))
        .route(
            "/services/:service",
            get(openapi_handler!(services::get_service)),
        )
        .route(
            "/services/:service",
            patch(openapi_handler!(services::modify_service)),
        )
        .route(
            "/services/:service/descriptors",
            get(openapi_handler!(services::list_service_descriptors)),
        )
        .route(
            "/services/:service/state",
            post(openapi_handler!(services::modify_service_state)),
        )
        .route(
            "/services/:service/methods",
            get(openapi_handler!(methods::list_service_methods)),
        )
        .route(
            "/services/:service/methods/:method",
            get(openapi_handler!(methods::get_service_method)),
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

/// Notifies the worker about schema changes. This method is best-effort and will not fail if the worker
/// could not be reached.
async fn notify_worker_about_schema_changes(
    schema_reader: &FileMetaReader,
    mut node_svc_client: NodeSvcClient<Channel>,
) -> Result<(), MetaApiError> {
    let schema_updates = schema_reader
        .read()
        .await
        .map_err(|err| MetaApiError::Meta(err.into()))?;

    // don't fail if the worker is not reachable
    let result = node_svc_client
        .update_schemas(UpdateSchemaRequest {
            schema_bin: bincode::serde::encode_to_vec(schema_updates, bincode::config::standard())
                .map_err(|err| MetaApiError::Generic(err.into()))?
                .into(),
        })
        .await;

    if let Err(err) = result {
        debug!("Failed notifying worker about schema changes: {err}");
    }

    Ok(())
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
