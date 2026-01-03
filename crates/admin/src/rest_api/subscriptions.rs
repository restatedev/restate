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
use restate_errors::warn_it;
use restate_types::identifiers::SubscriptionId;
use restate_types::schema::registry::MetadataService;

/// Create subscription
///
/// Creates a new subscription that connects an event source (e.g., a Kafka topic) to a Restate service handler.
/// For more information, see the [subscription documentation](https://docs.restate.dev/operate/invocation#managing-kafka-subscriptions).
#[utoipa::path(
    post,
    path = "/subscriptions",
    operation_id = "create_subscription",
    tag = "subscription",
    request_body = CreateSubscriptionRequest,
    responses(
        (status = 201, description = "Subscription created successfully", body = SubscriptionResponse, headers(
            ("Location" = String, description = "URI of the created subscription")
        )),
        MetaApiError
    )
)]
pub async fn create_subscription<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(payload): Json<CreateSubscriptionRequest>,
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

/// Get subscription
///
/// Returns the details of a specific subscription, including its source, sink, and configuration options.
#[utoipa::path(
    get,
    path = "/subscriptions/{subscription}",
    operation_id = "get_subscription",
    tag = "subscription",
    params(
        ("subscription" = String, Path, description = "Subscription identifier"),
    ),
    responses(
        (status = 200, description = "Subscription details including source, sink, and options", body = SubscriptionResponse),
        MetaApiError
    )
)]
pub async fn get_subscription<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
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

/// List subscriptions
///
/// Returns a list of all registered subscriptions, optionally filtered by source or sink.
#[utoipa::path(
    get,
    path = "/subscriptions",
    operation_id = "list_subscriptions",
    tag = "subscription",
    params(ListSubscriptionsParams),
    responses(
        (status = 200, description = "List of subscriptions matching the filter criteria", body = ListSubscriptionsResponse)
    )
)]
pub async fn list_subscriptions<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
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

/// Delete subscription
///
/// Deletes a subscription. This will stop events from the source from being forwarded to the sink.
#[utoipa::path(
    delete,
    path = "/subscriptions/{subscription}",
    operation_id = "delete_subscription",
    tag = "subscription",
    params(
        ("subscription" = String, Path, description = "Subscription identifier"),
    ),
    responses(
        (status = 202, description = "Subscription deletion accepted and will be processed asynchronously"),
        MetaApiError
    )
)]
pub async fn delete_subscription<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
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
