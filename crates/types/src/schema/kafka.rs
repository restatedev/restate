// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::config::KafkaClusterOptions;
use crate::schema::Redaction;
use crate::schema::info::SchemaInfo;
use crate::schema::subscriptions::Subscription;
use crate::time::MillisSinceEpoch;
use http::uri::Authority;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;

/// # Kafka cluster name
///
/// Valid name to use as a kafka cluster identifier. MUST conform a valid hostname format.
#[derive(
    Clone,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
    derive_more::Debug,
    derive_more::Display,
    Eq,
    PartialEq,
    Hash,
)]
#[cfg_attr(feature = "utoipa-schema", derive(::utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa-schema", schema(value_type = String, format = Hostname))]
#[debug("{}", _0)]
pub struct KafkaClusterName(String);

impl KafkaClusterName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for KafkaClusterName {
    type Err = http::uri::InvalidUri;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // We validate with Uri authority directly, as it needs to be a valid authority string.
        Authority::from_str(s).map(|a| KafkaClusterName(a.to_string()))
    }
}

impl Deref for KafkaClusterName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct KafkaCluster {
    pub name: KafkaClusterName,
    pub properties: HashMap<String, String>,
    pub created_at: MillisSinceEpoch,

    /// # Info
    ///
    /// List of configuration/deprecation information related to this cluster.
    #[serde(skip, default)] // Serde skip because we generate this at runtime
    pub info: Vec<SchemaInfo>,
}

impl KafkaCluster {
    pub fn new(name: KafkaClusterName, properties: HashMap<String, String>) -> Self {
        Self {
            name,
            properties,
            created_at: MillisSinceEpoch::now(),
            info: vec![],
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn created_at(&self) -> MillisSinceEpoch {
        self.created_at
    }

    pub(in crate::schema) fn properties_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.properties
    }

    pub(in crate::schema) fn info_mut(&mut self) -> &mut Vec<SchemaInfo> {
        &mut self.info
    }
}

pub trait KafkaClusterResolver {
    fn get_kafka_cluster(
        &self,
        cluster_name: &str,
        redact_secrets: Redaction,
    ) -> Option<KafkaCluster>;

    fn get_kafka_cluster_and_subscriptions(
        &self,
        cluster_name: &str,
        redact_secrets: Redaction,
    ) -> Option<(KafkaCluster, Vec<Subscription>)>;

    fn list_kafka_clusters(&self, redact_secrets: Redaction) -> Vec<KafkaCluster>;
}

impl From<KafkaClusterOptions> for KafkaCluster {
    fn from(
        KafkaClusterOptions {
            name,
            brokers,
            additional_options,
        }: KafkaClusterOptions,
    ) -> Self {
        let mut properties = additional_options;
        properties.insert("metadata.broker.list".to_string(), brokers.join(","));

        Self {
            name: KafkaClusterName(name),
            properties,
            created_at: MillisSinceEpoch::UNIX_EPOCH,
            info: vec![SchemaInfo::new(DEPRECATED_KAFKA_CLUSTER_INFO_MESSAGE)],
        }
    }
}

pub(in crate::schema) const DUPLICATED_KAFKA_CLUSTER_INFO_MESSAGE: &str = "A Kafka cluster with the same name exists in the static cluster configuration, please remove it from the static configuration as it's deprecated.";
pub(in crate::schema) const DEPRECATED_KAFKA_CLUSTER_INFO_MESSAGE: &str = "This Kafka cluster was configured in the static cluster configuration, this is deprecated. Please register it using the new Kafka Admin functionalities.";
