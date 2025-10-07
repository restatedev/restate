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
use std::path::PathBuf;
use std::time::Duration;

use http::{HeaderName, Uri};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tokio::sync::Semaphore;

use restate_time_util::{FriendlyDuration, NonZeroFriendlyDuration};

use super::QueryEngineOptions;

/// # Admin server options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "AdminOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct AdminOptions {
    /// # Endpoint address
    ///
    /// Address to bind for the Admin APIs.
    pub bind_address: SocketAddr,

    /// # Advertised Admin endpoint
    ///
    /// Optional advertised Admin API endpoint.
    #[serde(with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")]
    #[cfg_attr(feature = "schemars", schemars(with = "String", url))]
    pub advertised_admin_endpoint: Option<Uri>,

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

    /// # Log trim check interval
    ///
    /// Controls the interval at which cluster controller tries to trim the logs. Log trimming
    /// can be disabled by setting it to "0".
    ///
    /// Note that this is only the interval at which logs are checked, and does not guarantee that
    /// trim will be performed. The conditions for safely trim the log vary depending on the
    /// deployment. For single nodes, the log records must be durably persisted to disk. In
    /// distributed deployments, automatic trimming requires an external snapshot destination - see
    /// `worker.snapshots` for more.
    log_trim_check_interval: FriendlyDuration,

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

    pub fn log_trim_check_interval(&self) -> Option<Duration> {
        self.log_trim_check_interval.to_non_zero_std()
    }

    /// set derived values if they are not configured to reduce verbose configurations
    pub fn set_derived_values(&mut self) {
        // Only derive bind_address if it is not explicitly set
        let bind_address = if self.bind_address.ip().is_unspecified() {
            format!("127.0.0.1:{}", self.bind_address.port())
        } else {
            self.bind_address.to_string()
        };

        if self.advertised_admin_endpoint.is_none() {
            self.advertised_admin_endpoint = Some(
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

impl Default for AdminOptions {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9070".parse().unwrap(),
            advertised_admin_endpoint: None,
            // max is limited by Tower's LoadShedLayer.
            deployment_routing_headers: vec![],
            concurrent_api_requests_limit: None,
            query_engine: Default::default(),
            heartbeat_interval: NonZeroFriendlyDuration::from_millis_unchecked(1500),
            // check whether we can trim logs every hour
            log_trim_check_interval: FriendlyDuration::from_secs(60 * 60),
            #[cfg(any(test, feature = "test-util"))]
            disable_cluster_controller: false,
            disable_web_ui: false,
            storage_accounting_update_interval: None,
        }
    }
}
