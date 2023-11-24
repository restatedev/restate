// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

// Export schema types to be used by other crates without exposing the fact
// that we are using proxying to restate-schema-api or restate-types
pub use restate_schema_api::subscription::{
    ListSubscriptionFilter, Subscription, SubscriptionResolver,
};

#[serde_as]
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
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

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SubscriptionResponse {
    pub id: String,
    pub source: String,
    pub sink: String,
    pub options: HashMap<String, String>,
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

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ListSubscriptionsParams {
    pub sink: Option<String>,
    pub source: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ListSubscriptionsResponse {
    pub subscriptions: Vec<SubscriptionResponse>,
}
