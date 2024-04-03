// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// # Kafka cluster options
///
/// Configuration options to connect to a Kafka cluster.
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub struct KafkaClusterOptions {
    /// # Servers
    ///
    /// Initial list of brokers (host or host:port).
    pub(crate) brokers: Vec<String>,

    /// # Additional options
    ///
    /// Free floating list of kafka options in the same form of rdkafka. For more details on all the available options:
    /// https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub(crate) additional_options: HashMap<String, String>,
}

/// # Subscription options
#[derive(Debug, Default, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "SubscriptionOptions"))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct KafkaIngressOptions {
    /// # Kafka clusters
    ///
    /// Configuration parameters for the known kafka clusters
    #[serde(flatten)]
    pub(crate) clusters: HashMap<String, KafkaClusterOptions>,
}
