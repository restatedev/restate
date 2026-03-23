// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

use crate::subscriptions::SubscriptionResponse;
use restate_types::schema::kafka::{KafkaCluster, KafkaClusterName};

/// Create Kafka cluster request
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateKafkaClusterRequest {
    /// # Cluster Name
    ///
    /// Name for the Kafka cluster, used to identify this Kafka cluster configuration in subscriptions. Must be a valid hostname format.
    pub name: KafkaClusterName,
    /// # Properties
    ///
    /// Kafka cluster configuration properties. Must contain either
    /// 'bootstrap.servers' or 'metadata.broker.list'.
    ///
    /// For a full list of configuration properties, check the [librdkafka documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
    pub properties: HashMap<String, String>,
}

/// Update Kafka cluster request
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateKafkaClusterRequest {
    /// # Properties
    ///
    /// Updated Kafka cluster configuration properties. Must contain either
    /// 'bootstrap.servers' or 'metadata.broker.list'.
    ///
    /// For a full list of configuration properties, check the [librdkafka documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
    pub properties: HashMap<String, String>,
}

/// Kafka cluster simple response.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleKafkaClusterResponse {
    /// # Cluster Name
    ///
    /// Name for the Kafka cluster, used to identify this Kafka cluster configuration in subscriptions. Must be a valid hostname format.
    pub name: String,
    /// # Properties
    ///
    /// Properties for connecting to the kafka cluster.
    ///
    /// For a full list of configuration properties, check the [librdkafka documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
    pub properties: HashMap<String, String>,
    /// # Created at
    ///
    /// When the Kafka cluster configuration was created.
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    pub created_at: humantime::Timestamp,
}

impl From<KafkaCluster> for SimpleKafkaClusterResponse {
    fn from(cluster: KafkaCluster) -> Self {
        Self {
            name: cluster.name().to_string(),
            properties: cluster.properties().clone(),
            created_at: SystemTime::from(cluster.created_at).into(),
        }
    }
}

/// Kafka cluster details with subscriptions.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct KafkaClusterResponse {
    /// # Cluster Name
    ///
    /// Name for the Kafka cluster, used to identify this Kafka cluster configuration in subscriptions. Must be a valid hostname format.
    pub name: String,
    /// # Properties
    ///
    /// Properties for connecting to the kafka cluster.
    ///
    /// For a full list of configuration properties, check the [librdkafka documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
    pub properties: HashMap<String, String>,
    /// # Created at
    ///
    /// When the Kafka cluster configuration was created.
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schema", schema(value_type = String))]
    pub created_at: humantime::Timestamp,
    /// # Subscriptions
    ///
    /// Subscriptions to this Kafka cluster, returned only when `include_subscriptions` is enabled.
    pub subscriptions: Vec<SubscriptionResponse>,
}

/// List of all Kafka clusters.
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ListKafkaClustersResponse {
    pub clusters: Vec<SimpleKafkaClusterResponse>,
}
