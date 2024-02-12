// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

use restate_meta_rest_model::subscriptions::*;
use restate_schema_api::subscription::SubscriptionResolver;

use axum::extract::Query;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{http, Json};
use okapi_operation::*;
use restate_types::identifiers::SubscriptionId;

/// Create subscription.
#[openapi(
    summary = "Create subscription",
    description = "Create subscription.",
    operation_id = "create_subscription",
    tags = "subscription",
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
pub async fn create_subscription<W>(
    State(state): State<AdminServiceState<W>>,
    #[request_body(required = true)] Json(payload): Json<CreateSubscriptionRequest>,
) -> Result<impl axum::response::IntoResponse, MetaApiError> {
    let subscription = state
        .meta_handle()
        .create_subscription(
            // Do not allow users to create their own subscription ids.
            None, /* subscription_id */
            payload.source,
            payload.sink,
            payload.options,
        )
        .await?;

    Ok((
        StatusCode::CREATED,
        [(
            http::header::LOCATION,
            format!("/subscriptions/{}", subscription.id()),
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
pub async fn get_subscription<W>(
    State(state): State<AdminServiceState<W>>,
    Path(subscription_id): Path<SubscriptionId>,
) -> Result<Json<SubscriptionResponse>, MetaApiError> {
    let subscription = state
        .schemas()
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
pub async fn list_subscriptions<W>(
    State(state): State<AdminServiceState<W>>,
    Query(ListSubscriptionsParams { sink, source }): Query<ListSubscriptionsParams>,
) -> Json<ListSubscriptionsResponse> {
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

    ListSubscriptionsResponse {
        subscriptions: state
            .schemas()
            .list_subscriptions(&filters)
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
pub async fn delete_subscription<W>(
    State(state): State<AdminServiceState<W>>,
    Path(subscription_id): Path<SubscriptionId>,
) -> Result<StatusCode, MetaApiError> {
    state
        .meta_handle()
        .delete_subscription(subscription_id)
        .await?;

    Ok(StatusCode::ACCEPTED)
}
