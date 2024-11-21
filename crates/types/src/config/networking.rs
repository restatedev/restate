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
use std::time::Duration;

use crate::retries::RetryPolicy;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

/// # Networking options
///
/// Common network configuration options for communicating with Restate cluster nodes. Note that
/// similar keys are present in other config sections, such as in Service Client options.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "NetworkingOptions", default))]
#[builder(default)]
#[serde(rename_all = "kebab-case")]
pub struct NetworkingOptions {
    /// # Connect timeout
    ///
    /// TCP connection timeout for Restate cluster node-to-node network connections.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub connect_timeout: humantime::Duration,

    /// # Connect retry policy
    ///
    /// Retry policy to use for internal node-to-node networking.
    pub connect_retry_policy: RetryPolicy,

    /// # Handshake timeout
    ///
    /// Timeout for receiving a handshake response from Restate cluster peers.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub handshake_timeout: humantime::Duration,

    /// # HTTP/2 Keep Alive Interval
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub http2_keep_alive_interval: humantime::Duration,

    /// # HTTP/2 Keep Alive Timeout
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub http2_keep_alive_timeout: humantime::Duration,

    /// # HTTP/2 Adaptive Window
    pub http2_adaptive_window: bool,

    /// # Connection Send Buffer
    ///
    /// The number of messages that can be queued on the outbound stream of a single
    /// connection.
    pub outbound_queue_length: NonZeroUsize,
}

impl Default for NetworkingOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5).into(),
            connect_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(250),
                2.0,
                Some(10),
                Some(Duration::from_millis(3000)),
            ),
            handshake_timeout: Duration::from_secs(3).into(),
            outbound_queue_length: NonZeroUsize::new(1000).expect("Non zero number"),
            http2_keep_alive_interval: Duration::from_secs(40).into(),
            http2_keep_alive_timeout: Duration::from_secs(20).into(),
            http2_adaptive_window: true,
        }
    }
}
