// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::subscription_controller::Service;
use restate_ingress_dispatcher::IngressRequestSender;
use restate_schema_api::subscription::{Source, Subscription, SubscriptionValidator};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::warn;

/// # Kafka cluster options
///
/// Configuration options to connect to a Kafka cluster.
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
pub struct KafkaClusterOptions {
    /// # Servers
    ///
    /// Initial list of brokers as a CSV list of broker host or host:port.
    #[serde(rename = "metadata.broker.list")]
    #[serde(alias = "bootstrap.servers")]
    pub(crate) servers: String,

    /// # Additional options
    ///
    /// Free floating list of kafka options in the same form of rdkafka. For more details on all the available options:
    /// https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    #[serde(flatten)]
    pub(crate) additional_options: HashMap<String, String>,
}

/// # Subscription options
#[derive(Debug, Clone, Default, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "SubscriptionOptions"))]
#[builder(default)]
pub struct Options {
    /// # Kafka clusters
    ///
    /// Configuration parameters for the known kafka clusters
    pub(crate) clusters: HashMap<String, KafkaClusterOptions>,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid option '{name}'. Reason: {reason}")]
pub struct ValidationError {
    name: &'static str,
    reason: &'static str,
}

impl SubscriptionValidator for Options {
    type Error = ValidationError;

    fn validate(&self, mut subscription: Subscription) -> Result<Subscription, Self::Error> {
        // Retrieve the cluster option and merge them with subscription metadata
        let Source::Kafka { cluster, .. } = subscription.source();
        let cluster_options = &self.clusters.get(cluster).ok_or(ValidationError {
            name: "source",
            reason: "specified cluster in the source URI does not exist. Make sure it is defined in the KafkaOptions",
        })?.additional_options;

        if cluster_options.contains_key("enable.auto.commit")
            || subscription.metadata().contains_key("enable.auto.commit")
        {
            warn!("The configuration option enable.auto.commit should not be set and it will be ignored.");
        }
        if cluster_options.contains_key("enable.auto.offset.store")
            || subscription
                .metadata()
                .contains_key("enable.auto.offset.store")
        {
            warn!("The configuration option enable.auto.offset.store should not be set and it will be ignored.");
        }

        // Set the group.id if unset
        if !(cluster_options.contains_key("group.id")
            || subscription.metadata().contains_key("group.id"))
        {
            let group_id = subscription.id().to_string();

            subscription
                .metadata_mut()
                .insert("group.id".to_string(), group_id);
        }

        // Set client.id if unset
        if !(cluster_options.contains_key("client.id")
            || subscription.metadata().contains_key("client.id"))
        {
            subscription
                .metadata_mut()
                .insert("client.id".to_string(), "restate".to_string());
        }

        Ok(subscription)
    }
}

impl Options {
    pub fn build(self, tx: IngressRequestSender) -> Service {
        Service::new(self, tx)
    }
}
