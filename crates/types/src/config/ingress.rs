// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use super::KafkaClusterOptions;

/// # Ingress options
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "IngressOptions"))]
#[cfg_attr(feature = "schemars", schemars(default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct IngressOptions {
    /// # Bind address
    ///
    /// The address to bind for the ingress.
    pub bind_address: SocketAddr,

    /// # Concurrency limit
    ///
    /// Local concurrency limit to use to limit the amount of concurrent requests. If exceeded,
    /// the ingress will reply immediately with an appropriate status code. Default is unlimited.
    concurrent_api_requests_limit: Option<NonZeroUsize>,

    kafka_clusters: Vec<KafkaClusterOptions>,

    /// # Experimental feature to run the ingress independent of the worker role
    ///
    /// This feature is experimental and should be used with caution. It allows to run the ingress
    /// role independent of the worker role. It requires that you configure the [`Role::Ingress`]
    /// role for nodes explicitly. If you enable this feature, then you might not be able to roll
    /// back to a previous version.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub experimental_feature_enable_separate_ingress_role: bool,

    /// Cluster of new features for the kafka ingress, including:
    ///
    /// * New Source::Subscription
    /// * Shared handlers can now receive events https://github.com/restatedev/restate/issues/2100
    #[cfg_attr(feature = "schemars", schemars(skip))]
    experimental_feature_kafka_ingress_next: bool,
}

impl IngressOptions {
    pub fn set_bind_address(&mut self, bind_address: SocketAddr) {
        self.bind_address = bind_address
    }

    pub fn get_kafka_cluster(&self, name: &str) -> Option<&KafkaClusterOptions> {
        // a cluster is likely to have a very small number of kafka clusters configured.
        self.kafka_clusters.iter().find(|c| c.name == name)
    }

    pub fn available_kafka_clusters(&self) -> Vec<&str> {
        self.kafka_clusters
            .iter()
            .map(|c| c.name.as_str())
            .collect()
    }

    pub fn concurrent_api_requests_limit(&self) -> usize {
        std::cmp::min(
            self.concurrent_api_requests_limit
                .map(Into::into)
                .unwrap_or(Semaphore::MAX_PERMITS - 1),
            Semaphore::MAX_PERMITS - 1,
        )
    }

    pub fn experimental_feature_kafka_ingress_next(&self) -> bool {
        self.experimental_feature_kafka_ingress_next
    }
}

impl Default for IngressOptions {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".parse().unwrap(),
            // max is limited by Tower's LoadShedLayer.
            concurrent_api_requests_limit: None,
            kafka_clusters: Default::default(),
            experimental_feature_enable_separate_ingress_role: false,
            experimental_feature_kafka_ingress_next: false,
        }
    }
}
