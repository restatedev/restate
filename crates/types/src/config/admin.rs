// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::path::PathBuf;

use http::HeaderName;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::serde_as;
use tokio::sync::Semaphore;

use restate_util_time::NonZeroFriendlyDuration;

use super::{CommonOptions, ListenerOptions, QueryEngineOptions};
use crate::net::address::{AdminPort, AdvertisedAddress, BindAddress};
use crate::net::listener::AddressBook;

/// # Admin server options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "AdminOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct AdminOptions {
    /// Address to bind for the Admin APIs.
    #[serde(flatten)]
    admin_listener_options: ListenerOptions<AdminPort>,

    /// # Advertised Admin endpoint
    ///
    /// Optional advertised Admin API endpoint.
    /// [Deprecated] Use `advertised-address` instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) advertised_admin_endpoint: Option<AdvertisedAddress<AdminPort>>,

    /// # Advertised schema registry endpoint
    ///
    /// Absolute http(s) URL of the external schema registry, as reachable from the browser. Reported
    /// by `GET /version` for the Web UI; Restate never calls it.
    ///
    /// Since v1.8.1
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_absolute_http_url"
    )]
    pub experimental_advertised_schema_registry_endpoint: Option<String>,

    /// # Advertised ingress mux endpoint
    ///
    /// Absolute http(s) URL of the ingress mux, as reachable from the browser. Reported by
    /// `GET /version` for the Web UI; Restate never calls it.
    ///
    /// Since v1.8.1
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_absolute_http_url"
    )]
    pub experimental_advertised_ingress_mux_endpoint: Option<String>,

    /// # Deployment routing headers
    ///
    /// List of header names considered routing headers.
    ///
    /// These will be used during deployment creation to distinguish between an already existing deployment and a new deployment.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "serde_with::As::<Vec<restate_serde_util::HeaderNameSerde>>"
    )]
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub deployment_routing_headers: Vec<HeaderName>,

    /// # Concurrency limit
    ///
    /// Concurrency limit for the Admin APIs. Default is unlimited.
    concurrent_api_requests_limit: Option<NonZeroUsize>,
    pub query_engine: QueryEngineOptions,

    /// # Controller heartbeats
    ///
    /// Controls the interval at which cluster controller polls nodes of the cluster.
    pub heartbeat_interval: NonZeroFriendlyDuration,

    /// Disable serving the Restate Web UI on the admin port. Default is `false`.
    pub disable_web_ui: bool,

    #[cfg(any(test, feature = "test-util"))]
    pub disable_cluster_controller: bool,

    /// Enable state storage accounting. Default is `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub storage_accounting_update_interval: Option<NonZeroFriendlyDuration>,
}

impl AdminOptions {
    pub fn bind_address(&self) -> BindAddress<AdminPort> {
        self.admin_listener_options.bind_address()
    }

    pub fn admin_listener_options(&self) -> &ListenerOptions<AdminPort> {
        &self.admin_listener_options
    }

    pub fn advertised_address(&self, address_book: &AddressBook) -> AdvertisedAddress<AdminPort> {
        self.advertised_admin_endpoint
            .clone()
            .unwrap_or_else(|| self.admin_listener_options.advertised_address(address_book))
    }

    pub fn data_dir(&self) -> PathBuf {
        super::data_dir("registry")
    }

    pub fn concurrent_api_requests_limit(&self) -> usize {
        std::cmp::min(
            self.concurrent_api_requests_limit
                .map(Into::into)
                .unwrap_or(Semaphore::MAX_PERMITS - 1),
            Semaphore::MAX_PERMITS - 1,
        )
    }

    pub fn is_cluster_controller_enabled(&self) -> bool {
        #[cfg(not(any(test, feature = "test-util")))]
        return true;
        #[cfg(any(test, feature = "test-util"))]
        return !self.disable_cluster_controller;
    }

    /// set derived values if they are not configured to reduce verbose configurations
    pub fn set_derived_values(&mut self, common: &CommonOptions) {
        self.admin_listener_options
            .merge(common.fabric_listener_options());
    }
}

impl Default for AdminOptions {
    fn default() -> Self {
        Self {
            advertised_admin_endpoint: None,
            experimental_advertised_schema_registry_endpoint: None,
            experimental_advertised_ingress_mux_endpoint: None,
            admin_listener_options: Default::default(),
            // max is limited by Tower's LoadShedLayer.
            deployment_routing_headers: vec![],
            concurrent_api_requests_limit: None,
            query_engine: Default::default(),
            heartbeat_interval: NonZeroFriendlyDuration::from_millis_unchecked(1500),
            #[cfg(any(test, feature = "test-util"))]
            disable_cluster_controller: false,
            disable_web_ui: false,
            storage_accounting_update_interval: None,
        }
    }
}

fn deserialize_absolute_http_url<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let Some(url) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };
    let uri: http::Uri = url.parse().map_err(serde::de::Error::custom)?;
    if !matches!(uri.scheme_str(), Some("http" | "https")) || uri.authority().is_none() {
        return Err(serde::de::Error::custom(format!(
            "'{url}' is not an absolute http(s) URL"
        )));
    }
    Ok(Some(url))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn with_schema_registry_endpoint(url: &str) -> serde_json::Result<AdminOptions> {
        let mut value = serde_json::to_value(AdminOptions::default()).unwrap();
        value["experimental-advertised-schema-registry-endpoint"] = json!(url);
        serde_json::from_value(value)
    }

    #[test]
    fn advertised_endpoint_accepts_absolute_http_url() {
        let options = with_schema_registry_endpoint("https://registry.example.com:8443").unwrap();
        assert_eq!(
            options
                .experimental_advertised_schema_registry_endpoint
                .as_deref(),
            Some("https://registry.example.com:8443")
        );
    }

    #[test]
    fn advertised_endpoint_rejects_relative_url() {
        assert!(with_schema_registry_endpoint("/registry").is_err());
    }

    #[test]
    fn advertised_endpoint_rejects_non_http_scheme() {
        assert!(with_schema_registry_endpoint("ftp://registry.example.com").is_err());
    }
}
