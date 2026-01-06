// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::config::IngestionOptions;
use crate::net::address::{AdvertisedAddress, BindAddress, HttpIngressPort};
use crate::net::listener::AddressBook;

use super::{CommonOptions, KafkaClusterOptions, ListenerOptions};

/// # Ingress options
#[derive(Debug, Default, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "IngressOptions"))]
#[cfg_attr(feature = "schemars", schemars(default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct IngressOptions {
    #[serde(flatten)]
    ingress_listener_options: ListenerOptions<HttpIngressPort>,

    /// # Concurrency limit
    ///
    /// Local concurrency limit to use to limit the amount of concurrent requests. If exceeded,
    /// the ingress will reply immediately with an appropriate status code. Default is unlimited.
    concurrent_api_requests_limit: Option<NonZeroUsize>,

    kafka_clusters: Vec<KafkaClusterOptions>,

    /// # Ingress endpoint
    ///
    /// [Deprecated] Use `advertised-address` instead.
    /// Ingress endpoint that the Web UI should use to interact with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    advertised_ingress_endpoint: Option<AdvertisedAddress<HttpIngressPort>>,

    /// # Ingestion Options
    ///
    /// Settings for the ingestion client
    /// Currently only used by the Kafka ingress and the admin API.
    pub ingestion: IngestionOptions,
}

impl IngressOptions {
    pub fn bind_address(&self) -> BindAddress<HttpIngressPort> {
        self.ingress_listener_options.bind_address()
    }

    pub fn ingress_listener_options(&self) -> &ListenerOptions<HttpIngressPort> {
        &self.ingress_listener_options
    }

    pub fn advertised_address(
        &self,
        address_book: &AddressBook,
    ) -> AdvertisedAddress<HttpIngressPort> {
        self.advertised_ingress_endpoint.clone().unwrap_or_else(|| {
            self.ingress_listener_options
                .advertised_address(address_book)
        })
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
    pub fn set_derived_values(&mut self, common: &CommonOptions) {
        self.ingress_listener_options
            .merge(common.fabric_listener_options());
    }
}
