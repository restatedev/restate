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
use std::path::PathBuf;

use http::HeaderName;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tokio::sync::Semaphore;

use restate_util_time::{FriendlyDuration, NonZeroFriendlyDuration};

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

    /// # Reconfiguration grace period
    ///
    /// How long the cluster controller waits before moving a partition's replicas away from
    /// dead nodes. If a dead node comes back within this period (e.g. during a rolling restart),
    /// its partitions are not reconfigured, avoiding the cost of building new replicas elsewhere.
    /// Leadership still fails over to an alive replica immediately. If all replicas of a
    /// partition are on dead nodes, the partition is reconfigured without waiting. Set to `0s`
    /// to reconfigure as soon as a node is considered dead.
    ///
    /// Default: 5 minutes
    ///
    /// Since v1.8.2
    pub reconfiguration_grace_period: FriendlyDuration,

    /// # Leader handover policy
    ///
    /// When the cluster controller moves a partition's leadership to another replica while the
    /// current leader is healthy, for example after a node came back or joined the cluster.
    /// The partition has no leader until the new leader has replayed the log up to the point
    /// where it took over, so moving leadership to a replica that is far behind makes the
    /// partition unavailable for that long.
    ///
    /// * `catch-up-time`: move leadership only once the new leader is estimated to catch up with
    ///   the log within `leader-handover-max-catch-up-time`. The same applies to replica set
    ///   changes that remove the current leader.
    /// * `immediate`: move leadership as soon as the new leader has finished its initial replay.
    ///
    /// Leadership always fails over immediately when the current leader is down.
    ///
    /// Default: `catch-up-time`
    ///
    /// Since v1.8.2
    pub leader_handover_policy: LeaderHandoverPolicy,

    /// # Leader handover maximum catch-up time
    ///
    /// With the `catch-up-time` leader handover policy, a replica only takes over leadership from
    /// a healthy leader if it is estimated to replay the rest of the log within this time. The
    /// estimate is the number of records the replica is behind divided by its replay rate.
    ///
    /// Default: 3 seconds
    ///
    /// Since v1.8.2
    pub leader_handover_max_catch_up_time: NonZeroFriendlyDuration,

    /// # Leader handover probe interval
    ///
    /// With the `catch-up-time` leader handover policy, how often the cluster controller measures
    /// the replay rate of replicas and checks the progress of a replica that is catching up to take
    /// over leadership.
    ///
    /// Default: 10 seconds
    ///
    /// Since v1.8.2
    pub leader_handover_probe_interval: NonZeroFriendlyDuration,

    /// # Leader handover maximum attempts
    ///
    /// With the `catch-up-time` leader handover policy, how many rounds a replica gets to catch up
    /// with the log before leadership is moved to it anyway. A round ends when the replica has
    /// replayed the log up to where it was when the round started. A replica that falls further
    /// behind while it catches up gets all rounds as well.
    ///
    /// Default: 5
    ///
    /// Since v1.8.2
    pub leader_handover_max_attempts: NonZeroU32,

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
            admin_listener_options: Default::default(),
            // max is limited by Tower's LoadShedLayer.
            deployment_routing_headers: vec![],
            concurrent_api_requests_limit: None,
            query_engine: Default::default(),
            heartbeat_interval: NonZeroFriendlyDuration::from_millis_unchecked(1500),
            reconfiguration_grace_period: FriendlyDuration::from_secs(5 * 60),
            leader_handover_policy: LeaderHandoverPolicy::default(),
            leader_handover_max_catch_up_time: NonZeroFriendlyDuration::from_secs_unchecked(3),
            leader_handover_probe_interval: NonZeroFriendlyDuration::from_secs_unchecked(10),
            leader_handover_max_attempts: NonZeroU32::new(5).expect("is non-zero"),
            #[cfg(any(test, feature = "test-util"))]
            disable_cluster_controller: false,
            disable_web_ui: false,
            storage_accounting_update_interval: None,
        }
    }
}

/// # Leader handover policy
///
/// When the cluster controller moves a partition's leadership away from a healthy leader.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, strum::Display)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum LeaderHandoverPolicy {
    /// # Catch-up time
    ///
    /// Move leadership only to a replica that is estimated to catch up with the log within
    /// `leader-handover-max-catch-up-time`.
    #[default]
    CatchUpTime,
    /// # Immediate
    ///
    /// Move leadership as soon as a replica has finished its initial replay, whatever its lag.
    Immediate,
}
