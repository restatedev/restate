// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

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
    /// Local concurrency limit to use to limit the amount of concurrent requests. If exceeded, the ingress will reply immediately with an appropriate status code.
    /// Max allowed value is 2305843009213693950
    pub concurrent_api_requests_limit: usize,

    kafka_clusters: Vec<KafkaClusterOptions>,
}

impl IngressOptions {
    pub fn get_kafka_cluster(&self, name: &str) -> Option<&KafkaClusterOptions> {
        // a cluster is likely to have a very small number of kafka clusters configured.
        self.kafka_clusters.iter().find(|c| c.name == name)
    }
}

impl Default for IngressOptions {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".parse().unwrap(),
            // max is limited by Tower's LoadShedLayer.
            concurrent_api_requests_limit: Semaphore::MAX_PERMITS - 1,
            kafka_clusters: Default::default(),
        }
    }
}
