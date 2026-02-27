// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate service protocol, which is
// released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/restate/blob/main/LICENSE

use crate::config::KafkaClusterOptions;
use crate::schema::Redaction;
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
}

impl KafkaCluster {
    pub fn new(name: KafkaClusterName, properties: HashMap<String, String>) -> Self {
        Self {
            name,
            properties,
            created_at: MillisSinceEpoch::now(),
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
        }
    }
}
