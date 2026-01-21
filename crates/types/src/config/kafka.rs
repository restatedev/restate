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

use serde::{Deserialize, Serialize};

/// # Kafka cluster options
///
/// Configuration options to connect to a Kafka cluster.
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub struct KafkaClusterOptions {
    /// Cluster name (Used to identify subscriptions).
    pub name: String,
    /// # Servers
    ///
    /// Initial list of brokers (host or host:port).
    pub brokers: Vec<String>,

    /// # Additional options
    ///
    /// Free floating list of kafka options in the same form of rdkafka. For more details on all the available options:
    /// https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    #[serde(flatten, skip_serializing_if = "HashMap::is_empty")]
    pub additional_options: HashMap<String, String>,
}
