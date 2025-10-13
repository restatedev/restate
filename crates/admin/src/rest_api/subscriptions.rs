// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::error::*;
use crate::state::AdminServiceState;

use restate_admin_rest_model::subscriptions::*;
use restate_types::schema::subscriptions::ListSubscriptionFilter;

use axum::extract::Query;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Json, http};
use okapi_operation::*;
use restate_errors::warn_it;
use restate_types::identifiers::SubscriptionId;
use restate_types::schema::registry::MetadataService;

/// Create subscription.
#[openapi(
    summary = "Create subscription",
    description = "Create subscription.",
    operation_id = "create_subscription",
    tags = "subscription",
    external_docs(
        url = "https://docs.restate.dev/operate/invocation#managing-kafka-subscriptions"
    ),
    responses(
        ignore_return_type = true,
        response(
            status = "201",
            description = "Created",
            content = "Json<SubscriptionResponse>",
        ),
        from_type = "MetaApiError",
    )
)]
pub async fn create_subscription<Metadata, Discovery, Telemetry, Invocations>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations>>,
    #[request_body(required = true)] Json(payload): Json<CreateSubscriptionRequest>,
) -> Result<impl axum::response::IntoResponse, MetaApiError>
where
    Metadata: MetadataService,
{
    let subscription = state
        .schema_registry
        .create_subscription(payload.source, payload.sink, payload.options)
        .await
        .inspect_err(|e| warn_it!(e))?;

    Ok((
        StatusCode::CREATED,
        [(
            http::header::LOCATION,
            format!("subscriptions/{}", subscription.id()),
        )],
        Json(SubscriptionResponse::from(subscription)),
    ))
}

/// Get subscription.
#[openapi(
    summary = "Get subscription",
    description = "Get subscription",
    operation_id = "get_subscription",
    tags = "subscription",
    parameters(path(
        name = "subscription",
        description = "Subscription identifier",
        schema = "std::string::String"
    ))
)]
pub async fn get_subscription<Metadata, Discovery, Telemetry, Invocations>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations>>,
    Path(subscription_id): Path<SubscriptionId>,
) -> Result<Json<SubscriptionResponse>, MetaApiError>
where
    Metadata: MetadataService,
{
    let subscription = state
        .schema_registry
        .get_subscription(subscription_id)
        .ok_or_else(|| MetaApiError::SubscriptionNotFound(subscription_id))?;

    Ok(SubscriptionResponse::from(subscription).into())
}

/// List subscriptions.
#[openapi(
    summary = "List subscriptions",
    description = "List all subscriptions.",
    operation_id = "list_subscriptions",
    tags = "subscription",
    parameters(
        query(
            name = "sink",
            description = "Filter by the exact specified sink.",
            required = false,
            style = "simple",
            allow_empty_value = false,
            schema = "String",
        ),
        query(
            name = "source",
            description = "Filter by the exact specified source.",
            required = false,
            style = "simple",
            allow_empty_value = false,
            schema = "String",
        )
    )
)]
pub async fn list_subscriptions<Metadata, Discovery, Telemetry, Invocations>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations>>,
    Query(ListSubscriptionsParams { sink, source }): Query<ListSubscriptionsParams>,
) -> Json<ListSubscriptionsResponse>
where
    Metadata: MetadataService,
{
    let filters = match (sink, source) {
        (Some(sink_filter), Some(source_filter)) => vec![
            ListSubscriptionFilter::ExactMatchSink(sink_filter),
            ListSubscriptionFilter::ExactMatchSource(source_filter),
        ],
        (Some(sink_filter), None) => vec![ListSubscriptionFilter::ExactMatchSink(sink_filter)],
        (None, Some(source_filter)) => {
            vec![ListSubscriptionFilter::ExactMatchSource(source_filter)]
        }
        _ => vec![],
    };

    let subscriptions = state.schema_registry.list_subscriptions(&filters);

    ListSubscriptionsResponse {
        subscriptions: subscriptions
            .into_iter()
            .map(SubscriptionResponse::from)
            .collect(),
    }
    .into()
}

/// Delete subscription.
#[openapi(
    summary = "Delete subscription",
    description = "Delete subscription.",
    operation_id = "delete_subscription",
    tags = "subscription",
    parameters(path(
        name = "subscription",
        description = "Subscription identifier",
        schema = "std::string::String"
    )),
    responses(
        ignore_return_type = true,
        response(
            status = "202",
            description = "Accepted",
            content = "okapi_operation::Empty",
        ),
        from_type = "MetaApiError",
    )
)]
pub async fn delete_subscription<Metadata, Discovery, Telemetry, Invocations>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations>>,
    Path(subscription_id): Path<SubscriptionId>,
) -> Result<StatusCode, MetaApiError>
where
    Metadata: MetadataService,
{
    state
        .schema_registry
        .delete_subscription(subscription_id)
        .await
        .inspect_err(|e| warn_it!(e))?;
    Ok(StatusCode::ACCEPTED)
}
