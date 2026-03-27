// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use serde_with::serde_as;
use std::collections::HashMap;

/// The schema information
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    // Registered deployments
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deployments_v2: Option<Vec<Deployment>>,

    // --- Schema data structure
    /// This gets bumped on each update.
    version: Version,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    subscriptions: HashMap<SubscriptionId, Subscription>,
}

impl From<super::Schema> for Schema {
    fn from(
        super::Schema {
            version,
            deployments,
            subscriptions,
            ..
        }: super::Schema,
    ) -> Self {
        Self {
            deployments_v2: Some(deployments.into_values().collect()),
            version,
            subscriptions,
        }
    }
}

impl From<Schema> for super::Schema {
    fn from(
        Schema {
            deployments_v2,
            version,
            subscriptions,
        }: Schema,
    ) -> Self {
        if let Some(deployments_v2) = deployments_v2 {
            Self {
                version,
                active_service_revisions: ActiveServiceRevision::create_index(&deployments_v2),
                deployments: deployments_v2
                    .into_iter()
                    .map(|deployment| (deployment.id, deployment))
                    .collect(),
                subscriptions,
            }
        } else {
            panic!(
                "Detected legacy v1 schema format which is no longer supported since Restate v1.5. \
                Please downgrade to v1.5, perform a schema registry mutation (e.g., add a header to a deployment), \
                then upgrade again to v1.7."
            )
        }
    }
}
