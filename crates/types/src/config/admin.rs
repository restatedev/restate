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

use http::Uri;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tokio::sync::Semaphore;

use super::{print_warning_deprecated_config_option, QueryEngineOptions};
use crate::partition_table::PartitionReplication;

/// # Admin server options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "AdminOptions", default))]
#[serde(rename_all = "kebab-case", from = "AdminOptionsShadow")]
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

    /// # Log trim check interval
    ///
    /// Controls the interval at which cluster controller tries to trim the logs. Log trimming
    /// can be disabled by setting it to "0s".
    ///
    /// Note that this is only the interval at which logs are checked, and does not guarantee that
    /// trim will be performed. To safely trim the log, the log records must be known to be
    /// persisted by the corresponding partition processor(s). For single server deployments, use
    /// the `persist-lsn-*` settings in `worker.storage`. In distributed deployments, this is
    /// accomplished by configuring an external snapshot destination - see `worker.snapshots` for
    /// more.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    log_trim_check_interval: humantime::Duration,

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

    pub fn log_trim_check_interval(&self) -> Option<Duration> {
        if self.log_trim_check_interval.is_zero() {
            None
        } else {
            Some(*self.log_trim_check_interval)
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
            // check whether we can trim logs every hour
            log_trim_check_interval: Duration::from_secs(60 * 60).into(),
            log_trim_threshold: None,
            default_partition_replication: PartitionReplication::default(),
            #[cfg(any(test, feature = "test-util"))]
            disable_cluster_controller: false,
            log_tail_update_interval: Duration::from_secs(5 * 60).into(),
            advertised_ingress_endpoint: Some("http://localhost:8080/".parse().unwrap()),
        }
    }
}

impl From<AdminOptionsShadow> for AdminOptions {
    fn from(value: AdminOptionsShadow) -> Self {
        let log_trim_check_interval = value
            .log_trim_interval
            .inspect(|_| {
                print_warning_deprecated_config_option(
                    "log-trim-interval",
                    Some("admin.log-trim-check-interval"),
                )
            })
            .unwrap_or(value.log_trim_check_interval);

        Self {
            bind_address: value.bind_address,
            concurrent_api_requests_limit: value.concurrent_api_requests_limit,
            query_engine: value.query_engine,
            heartbeat_interval: value.heartbeat_interval,
            log_trim_check_interval,
            log_trim_threshold: value.log_trim_threshold,
            log_tail_update_interval: value.log_tail_update_interval,
            default_partition_replication: value.default_partition_replication,
            #[cfg(any(test, feature = "test-util"))]
            disable_cluster_controller: value.disable_cluster_controller,
            advertised_ingress_endpoint: value.advertised_ingress_endpoint,
        }
    }
}

/// Used to deserialize the [`AdminOptions`] in backwards compatible way.
///
/// | Current Name              | Backwards Compatible Aliases  | Since   |
/// |---------------------------|-------------------------------|---------|
/// | `log_trim_check_interval` |`log_trim_interval`            | 1.2     |
///
/// Once we no longer support the backwards compatible aliases, we can remove this struct.
#[serde_as]
#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct AdminOptionsShadow {
    bind_address: SocketAddr,
    concurrent_api_requests_limit: Option<NonZeroUsize>,
    query_engine: QueryEngineOptions,

    #[serde_as(as = "serde_with::DisplayFromStr")]
    heartbeat_interval: humantime::Duration,

    #[serde_as(as = "serde_with::DisplayFromStr")]
    log_trim_check_interval: humantime::Duration,

    // todo: drop in version 1.3
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    log_trim_interval: Option<humantime::Duration>,

    log_trim_threshold: Option<u64>,

    #[serde_as(as = "serde_with::DisplayFromStr")]
    log_tail_update_interval: humantime::Duration,

    default_partition_replication: PartitionReplication,

    #[cfg(any(test, feature = "test-util"))]
    disable_cluster_controller: bool,

    #[serde(with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")]
    advertised_ingress_endpoint: Option<Uri>,
}
