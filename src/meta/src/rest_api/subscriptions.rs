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
use super::state::*;

use axum::extract::Query;
use axum::extract::{Path, State};
use axum::http::{StatusCode, Uri};
use axum::{http, Json};
use okapi_operation::*;
use restate_schema_api::subscription::{
    ListSubscriptionFilter, Subscription, SubscriptionResolver,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;
use std::sync::Arc;

#[serde_as]
#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateSubscriptionRequest {
    /// # Identifier
    ///
    /// Identifier of the subscription. If not specified, one will be auto-generated.
    pub id: Option<String>,
    /// # Source
    ///
    /// Source uri. Accepted forms:
    ///
    /// * `kafka://<cluster_name>/<topic_name>`, e.g. `service://my-cluster/my-topic`
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[schemars(with = "String")]
    pub source: Uri,
    /// # Sink
    ///
    /// Sink uri. Accepted forms:
    ///
    /// * `service://<service_name>/<method_name>`, e.g. `service://com.example.MySvc/MyMethod`
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[schemars(with = "String")]
    pub sink: Uri,
    /// # Options
    ///
    /// Additional options to apply to the subscription.
    pub options: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SubscriptionResponse {
    id: String,
    source: String,
    sink: String,
    options: HashMap<String, String>,
}

impl From<Subscription> for SubscriptionResponse {
    fn from(value: Subscription) -> Self {
        Self {
            id: value.id().to_string(),
            source: value.source().to_string(),
            sink: value.sink().to_string(),
            options: value.metadata().clone(),
        }
    }
}

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
pub async fn create_subscription<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    #[request_body(required = true)] Json(payload): Json<CreateSubscriptionRequest>,
) -> Result<impl axum::response::IntoResponse, MetaApiError>
where
    S: SubscriptionResolver,
{
    let subscription = state
        .meta_handle()
        .create_subscription(payload.id, payload.source, payload.sink, payload.options)
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
pub async fn get_subscription<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(subscription_id): Path<String>,
) -> Result<Json<SubscriptionResponse>, MetaApiError>
where
    S: SubscriptionResolver,
{
    let subscription = state
        .schemas()
        .get_subscription(&subscription_id)
        .ok_or_else(|| MetaApiError::SubscriptionNotFound(subscription_id.clone()))?;

    Ok(SubscriptionResponse::from(subscription).into())
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListSubscriptionsParams {
    sink: Option<String>,
    source: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct ListSubscriptionsResponse {
    subscriptions: Vec<SubscriptionResponse>,
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
pub async fn list_subscriptions<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Query(ListSubscriptionsParams { sink, source }): Query<ListSubscriptionsParams>,
) -> Json<ListSubscriptionsResponse>
where
    S: SubscriptionResolver,
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
pub async fn delete_subscription<S, W>(
    State(state): State<Arc<RestEndpointState<S, W>>>,
    Path(subscription_id): Path<String>,
) -> Result<StatusCode, MetaApiError> {
    state
        .meta_handle()
        .delete_subscription(subscription_id)
        .await?;

    Ok(StatusCode::ACCEPTED)
}
