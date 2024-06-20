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
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_types::identifiers::SubscriptionId;
use restate_types::schema::subscriptions::Subscription;

#[serde_as]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateSubscriptionRequest {
    /// # Source
    ///
    /// Source uri. Accepted forms:
    ///
    /// * `kafka://<cluster_name>/<topic_name>`, e.g. `kafka://my-cluster/my-topic`
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    pub source: Uri,
    /// # Sink
    ///
    /// Sink uri. Accepted forms:
    ///
    /// * `service://<service_name>/<service_name>`, e.g. `service://Counter/count`
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    pub sink: Uri,
    /// # Options
    ///
    /// Additional options to apply to the subscription.
    pub options: Option<HashMap<String, String>>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
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

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Deserialize, Serialize)]
pub struct ListSubscriptionsParams {
    pub sink: Option<String>,
    pub source: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Debug, Deserialize, Serialize)]
pub struct ListSubscriptionsResponse {
    pub subscriptions: Vec<SubscriptionResponse>,
}
