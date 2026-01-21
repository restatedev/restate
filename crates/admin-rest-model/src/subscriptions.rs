// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use http::Uri;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_types::identifiers::SubscriptionId;
use restate_types::schema::subscriptions::Subscription;

#[serde_as]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateSubscriptionRequest {
    /// # Source
    ///
    /// Source uri. Accepted forms:
    ///
    /// * `kafka://<cluster_name>/<topic_name>`, e.g. `kafka://my-cluster/my-topic`
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schema", schema(value_type = String, format = "uri"))]
    pub source: Uri,
    /// # Sink
    ///
    /// Sink uri. Accepted forms:
    ///
    /// * `service://<service_name>/<service_name>`, e.g. `service://Counter/count`
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schema", schema(value_type = String, format = "uri"))]
    pub sink: Uri,
    /// # Options
    ///
    /// Additional options to apply to the subscription.
    pub options: Option<HashMap<String, String>>,
}

/// Subscription details.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Deserialize, Serialize)]
pub struct SubscriptionResponse {
    pub id: SubscriptionId,
    pub source: String,
    pub sink: String,
    pub options: HashMap<String, String>,
}

impl From<Subscription> for SubscriptionResponse {
    fn from(value: Subscription) -> Self {
        Self {
            id: value.id(),
            source: value.source().to_string(),
            sink: value.sink().to_string(),
            options: value.metadata().clone(),
        }
    }
}

#[cfg_attr(feature = "schema", derive(utoipa::IntoParams))]
#[derive(Debug, Deserialize, Serialize)]
pub struct ListSubscriptionsParams {
    /// Filter by the exact specified sink.
    pub sink: Option<String>,
    /// Filter by the exact specified source.
    pub source: Option<String>,
}

/// List of all subscriptions.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Deserialize, Serialize)]
pub struct ListSubscriptionsResponse {
    pub subscriptions: Vec<SubscriptionResponse>,
}
