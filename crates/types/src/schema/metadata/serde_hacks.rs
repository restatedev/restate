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

use std::collections::HashMap;

use serde_with::serde_as;

/// The schema information
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Schema {
    // Registered deployments
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deployments_v2: Option<Vec<Deployment>>,

    /// This gets bumped on each update.
    version: Version,
    // flexbuffers only supports string-keyed maps :-( --> so we store it as vector of kv pairs
    #[serde_as(as = "serde_with::Seq<(_, _)>")]
    subscriptions: HashMap<SubscriptionId, Subscription>,

    // Kafka clusters
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[serde_as(as = "restate_serde_util::MapAsVec")]
    kafka_clusters: HashMap<String, KafkaCluster>,
}

impl restate_serde_util::MapAsVecItem for KafkaCluster {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.to_string()
    }
}

impl From<super::Schema> for Schema {
    fn from(
        super::Schema {
            version,
            deployments,
            subscriptions,
            kafka_clusters,
            ..
        }: super::Schema,
    ) -> Self {
        Self {
            deployments_v2: Some(deployments.into_values().collect()),
            version,
            subscriptions,
            kafka_clusters,
        }
    }
}

impl From<Schema> for super::Schema {
    fn from(
        Schema {
            deployments_v2,
            version,
            subscriptions,
            kafka_clusters,
        }: Schema,
    ) -> Self {
        let deployments_v2 = deployments_v2
            .expect("schema V1 is no longer supported; upgrade via Restate v1.7 first");
        Self {
            version,
            active_service_revisions: ActiveServiceRevision::create_index(&deployments_v2),
            deployments: deployments_v2
                .into_iter()
                .map(|deployment| (deployment.id, deployment))
                .collect(),
            subscriptions,
            kafka_clusters,
        }
    }
}
