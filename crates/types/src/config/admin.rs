// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition_table::PartitionReplication;

use super::QueryEngineOptions;
use http::Uri;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::Semaphore;

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

    /// # Concurrency limit
    ///
    /// Concurrency limit for the Admin APIs. Default is unlimited.
    concurrent_api_requests_limit: Option<NonZeroUsize>,
    pub query_engine: QueryEngineOptions,

    /// # Controller heartbeats
    ///
    /// Controls the interval at which cluster controller polls nodes of the cluster.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub heartbeat_interval: humantime::Duration,

    /// # Log trim interval
    ///
    /// Controls the interval at which cluster controller tries to trim the logs. Log trimming
    /// can be disabled by setting it to "0s".
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    log_trim_interval: humantime::Duration,

    /// # Log trim threshold (deprecated)
    ///
    /// This configuration option is deprecated and ignored in Restate >= 1.2.
    pub log_trim_threshold: Option<u64>,

    /// # Log Tail Update interval
    ///
    /// Controls the interval at which cluster controller tries to refind the tails of logs. This
    /// is a safety-net check in case of a concurrent cluster controller crash.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub log_tail_update_interval: humantime::Duration,

    /// # Default partition replication factor
    ///
    /// [__PREVIEW FEATURE__]
    /// The default replication factor for partition processors, this impacts how many replicas
    /// each partition will have across the worker nodes of the cluster.
    ///
    /// Note that this value only impacts the cluster initial provisioning and will not be respected after
    /// the cluster has been provisioned.
    ///
    /// To update existing clusters use the `restatectl` utility.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub default_partition_replication: PartitionReplication,

    #[cfg(any(test, feature = "test-util"))]
    pub disable_cluster_controller: bool,

    /// # Ingress endpoint
    ///
    /// Ingress endpoint that the Web UI should use to interact with.
    #[serde(with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")]
    #[cfg_attr(feature = "schemars", schemars(with = "String", url))]
    pub advertised_ingress_endpoint: Option<Uri>,
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

    pub fn log_trim_interval(&self) -> Option<std::time::Duration> {
        if self.log_trim_interval.is_zero() {
            None
        } else {
            Some(*self.log_trim_interval)
        }
    }
}

impl Default for AdminOptions {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9070".parse().unwrap(),
            // max is limited by Tower's LoadShedLayer.
            concurrent_api_requests_limit: None,
            query_engine: Default::default(),
            heartbeat_interval: Duration::from_millis(1500).into(),
            // try to trim the log every hour
            log_trim_interval: Duration::from_secs(60 * 60).into(),
            log_trim_threshold: None,
            default_partition_replication: PartitionReplication::default(),
            #[cfg(any(test, feature = "test-util"))]
            disable_cluster_controller: false,
            log_tail_update_interval: Duration::from_secs(5 * 60).into(),
            advertised_ingress_endpoint: Some("http://localhost:8080/".parse().unwrap()),
        }
    }
}
