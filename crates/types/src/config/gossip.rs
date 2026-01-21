// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_time_util::{FriendlyDuration, NonZeroFriendlyDuration};

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "GossipOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
// NOTE: Prefix with gossip_
pub struct GossipOptions {
    /// # Gossip tick interval
    ///
    /// The interval at which the failure detector will tick. Decrease this value for faster reaction
    /// to node failures. Note, that every tick comes with an overhead.
    pub gossip_tick_interval: NonZeroFriendlyDuration,

    /// # Gossip failure threshold
    ///
    /// Specifies how many gossip intervals of inactivity need to pass before
    /// considering a node as dead.
    pub gossip_failure_threshold: NonZeroU32,

    /// # Number of peers to gossip
    ///
    /// On every gossip interval, how many peers each node attempts to gossip with. The default is
    /// optimized for small clusters (less than 5 nodes). On larger clusters, if gossip overhead is noticeable,
    /// consider reducing this value to 1.
    pub gossip_num_peers: NonZeroU32,

    /// # Gossips before failure detector is stable
    ///
    // After receiving how many gossips, should the failure detector consider its view of
    // the cluster health as stable. Before the stability period, the failure detector
    // will not perform its detection to reduce misjudgements that may happen on start up.
    pub gossip_fd_stability_threshold: NonZeroU32,

    /// # Suspect duration
    ///
    /// How long to keep a node in a transient state (suspect) before marking it as available.
    /// Larger values mean that the cluster is less prone to flaky nodes but extends the
    /// time it takes for a node to participate.
    ///
    /// A node becomes a suspect if it has been previously marked as dead for a given generation
    /// number. If the node incremented its generation number, it will not be impacted by this
    /// threshold.
    pub gossip_suspect_interval: FriendlyDuration,

    /// # Gossip loneliness threshold
    ///
    /// How many intervals need to pass without receiving any gossip messages before considering
    /// this node as potentially isolated/dead. This threshold is used in the case where the node
    /// can still send gossip messages but did not receive any. This can rarely happen in
    /// asymmetric network partitions.
    ///
    /// In this case, the node will advertise itself as dead in the gossip messages it sends out.
    ///
    /// Note: this threshold does not apply to a cluster that's configured with a single node.
    pub gossip_loneliness_threshold: NonZeroU32,

    /// # Gossip extras exchange frequency
    ///
    /// In addition to basic health/liveness information, the gossip protocol is used to exchange
    /// extra information about the roles hosted by this node. For instance, which partitions are
    /// currently running, their configuration versions, and the durable LSN of the corresponding
    /// partition databases. This information is sent every Nth gossip message. This setting
    /// controls the frequency of this exchange. For instance, `10` means that every 10th gossip
    /// message will contain the extra information about.
    pub gossip_extras_exchange_frequency: NonZeroU32,

    /// # Gossips time skew threshold
    ///
    /// The time skew is the maximum acceptable time difference between the local node and the time
    /// reported by peers via gossip messages. The time skew is also used to ignore gossip messages
    /// that are too old.
    pub gossip_time_skew_threshold: NonZeroFriendlyDuration,
}

// everything * 5
impl Default for GossipOptions {
    fn default() -> Self {
        Self {
            gossip_failure_threshold: NonZeroU32::new(10).expect("be non zero"),
            gossip_fd_stability_threshold: NonZeroU32::new(3).expect("be non zero"),
            gossip_tick_interval: NonZeroFriendlyDuration::from_millis_unchecked(100),
            gossip_num_peers: NonZeroU32::new(2).expect("be non zero"),
            gossip_suspect_interval: FriendlyDuration::from_secs(5),
            gossip_loneliness_threshold: NonZeroU32::new(30).expect("be non zero"),
            gossip_extras_exchange_frequency: NonZeroU32::new(10).expect("be non zero"),
            gossip_time_skew_threshold: NonZeroFriendlyDuration::from_millis_unchecked(1000),
        }
    }
}
