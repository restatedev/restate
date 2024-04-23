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
}

impl IngressOptions {
    pub fn get_kafka_cluster(&self, name: &str) -> Option<&KafkaClusterOptions> {
        // a cluster is likely to have a very small number of kafka clusters configured.
        self.kafka_clusters.iter().find(|c| c.name == name)
    }

    pub fn concurrent_api_requests_limit(&self) -> usize {
        std::cmp::min(
            self.concurrent_api_requests_limit
                .map(Into::into)
                .unwrap_or(Semaphore::MAX_PERMITS - 1),
            Semaphore::MAX_PERMITS - 1,
        )
    }
}

impl Default for IngressOptions {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".parse().unwrap(),
            // max is limited by Tower's LoadShedLayer.
            concurrent_api_requests_limit: None,
            kafka_clusters: Default::default(),
        }
    }
}
