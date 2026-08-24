// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU32, NonZeroUsize};

use restate_memory::NonZeroByteCount;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::config::{DEFAULT_MESSAGE_SIZE_LIMIT, IngestionOptions, NetworkingOptions};
use crate::net::address::{AdvertisedAddress, BindAddress, HttpIngressPort};
use crate::net::listener::AddressBook;

use super::{CommonOptions, KafkaClusterOptions, ListenerOptions};

/// # Ingestion API options
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(rename = "IngestionApiOptions", default)
)]
#[serde(default, rename_all = "kebab-case")]
pub struct IngestionApiOptions {
    /// # Disable the gRPC ingestion API
    ///
    /// Disable the experimental gRPC ingestion API on the ingress endpoint.
    ///
    /// Since v1.8.0
    pub disable: bool,

    /// # Maximum ingestion window size
    ///
    /// Maximum number of bytes an ingestion stream may have in flight before the server
    /// applies back pressure.
    ///
    /// Value is clipped at [`u32::MAX`]
    ///
    /// Since v1.8.0
    max_window_size: NonZeroByteCount,

    /// # Maximum concurrent streams
    ///
    /// Maximum number of ingestion streams may have in flight before the server
    /// start rejecting them
    ///
    /// Since v1.8.0
    max_concurrent_streams: NonZeroUsize,
}

impl IngestionApiOptions {
    pub fn max_window_size(&self) -> NonZeroU32 {
        let value = self.max_window_size.as_u64().min(u32::MAX as u64) as u32;
        NonZeroU32::new(value).expect("byte count is non-zero")
    }

    pub fn max_concurrent_streams(&self) -> usize {
        self.max_concurrent_streams
            .get()
            .min(Semaphore::MAX_PERMITS - 1)
    }
}

impl Default for IngestionApiOptions {
    fn default() -> Self {
        Self {
            disable: false,
            max_window_size: NonZeroByteCount::new(NonZeroUsize::new(128 * 1024).unwrap()),
            max_concurrent_streams: NonZeroUsize::new(1000).unwrap(),
        }
    }
}

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

    /// # HTTP/2 max concurrent streams
    ///
    /// Caps the number of concurrent HTTP/2 streams accepted per inbound ingress connection.
    /// If unset, Restate does not configure this limit and leaves it at hyper's runtime default.
    /// With the current hyper version, that default is 200 streams.
    /// Service-mesh clients such as Linkerd honor the advertised value as a hard per-connection
    /// concurrency limit, so high-concurrency or long-poll deployments may need to raise it.
    ///
    /// Since v1.7.0
    #[serde(skip_serializing_if = "Option::is_none")]
    http2_max_concurrent_streams: Option<NonZeroU32>,

    /// # Kafka clusters
    ///
    /// **Deprecated in 1.7**: Kafka clusters should now be configured through the UI/Admin API
    pub kafka_clusters: Vec<KafkaClusterOptions>,

    /// # Ingress endpoint
    ///
    /// [Deprecated] Use `advertised-address` instead.
    /// Ingress endpoint that the Web UI should use to interact with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    advertised_ingress_endpoint: Option<AdvertisedAddress<HttpIngressPort>>,

    /// # Request size limit
    ///
    /// Maximum size of request that can be received over ingress. If a request size is
    /// larger than this limit, the request will fail.
    ///
    /// If unset, defaults to `networking.message-size-limit`. If set, it will be clamped at
    /// the value of `networking.message-size-limit` since larger requests cannot be transmitted
    /// over the cluster internal network.
    ///
    /// Since v1.7.0
    #[serde(skip_serializing_if = "Option::is_none")]
    request_size_limit: Option<NonZeroByteCount>,

    /// # Ingestion Options
    ///
    /// Settings for the ingestion client
    /// Currently only used by the Kafka ingress and the admin API.
    pub ingestion: IngestionOptions,

    /// # Ingestion API options
    ///
    /// Settings for the experimental gRPC ingestion API.
    pub ingestion_api: IngestionApiOptions,
}

impl IngressOptions {
    pub fn request_size_limit(&self) -> NonZeroUsize {
        self.request_size_limit
            .map(|v| v.as_non_zero_usize())
            .unwrap_or(DEFAULT_MESSAGE_SIZE_LIMIT)
    }

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

    pub fn http2_max_concurrent_streams(&self) -> Option<NonZeroU32> {
        self.http2_max_concurrent_streams
    }

    /// set derived values if they are not configured to reduce verbose configurations
    pub fn set_derived_values(&mut self, common: &CommonOptions, networking: &NetworkingOptions) {
        self.ingress_listener_options
            .merge(common.fabric_listener_options());

        self.merge(networking);
    }

    fn merge(&mut self, opts: &NetworkingOptions) {
        self.request_size_limit = Some(
            self.request_size_limit
                .map(|limit| limit.min(opts.message_size_limit))
                .unwrap_or(opts.message_size_limit),
        );
    }
}
