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

use http::Uri;
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
    /// This feature is experimental and should be used with caution. It enables the HTTP ingress
    /// to run role independently of the worker role. If you activate this feature, you will need
    /// to specify the [`Role::HttpIngress`] role for nodes explicitly, and nodes that run
    /// only [`Role::Worker`] will not accept HTTP ingress requests. If you enable this feature,
    /// then you might not be able to roll back to a previous version.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub experimental_feature_enable_separate_ingress_role: bool,

    /// # Ingress endpoint
    ///
    /// Ingress endpoint that the Web UI should use to interact with.
    #[serde(with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")]
    #[cfg_attr(feature = "schemars", schemars(with = "String", url))]
    pub advertised_ingress_endpoint: Option<Uri>,
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

    /// set derived values if they are not configured to reduce verbose configurations
    pub fn set_derived_values(&mut self) {
        // Only derive bind_address if it is not explicitly set
        let bind_address = if self.bind_address.ip().is_unspecified() {
            format!("127.0.0.1:{}", self.bind_address.port())
        } else {
            self.bind_address.to_string()
        };

        if self.advertised_ingress_endpoint.is_none() {
            self.advertised_ingress_endpoint = Some(
                Uri::builder()
                    .scheme("http")
                    .authority(bind_address)
                    .path_and_query("/")
                    .build()
                    .expect("valid bind address"),
            );
        }
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
            advertised_ingress_endpoint: None,
        }
    }
}
