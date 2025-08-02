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

use restate_serde_util::NonZeroByteCount;

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

    /// # Disable Compression
    ///
    /// Disables Zstd compression for internal gRPC network connections
    pub disable_compression: bool,

    /// # Data Stream Window Size
    ///
    /// Controls the number of bytes the can be sent on every data stream before inducing
    /// back pressure. Data streams are used for sending messages between nodes.
    ///
    /// The value should is often derived from BDP (Bandwidth Delay Product) of the network. For
    /// instance, if the network has a bandwidth of 10 Gbps with a round-trip time of 5 ms, the BDP
    /// is 10 Gbps * 0.005 s = 6.25 MB. This means that the window size should be at least 6.25 MB
    /// to fully utilize the network bandwidth assuming the latency is constant. Our recommendation
    /// is to set the window size to 2x the BDP to account for any variations in latency.
    ///
    /// If network latency is high, it's recommended to set this to a higher value.
    /// Maximum theoretical value is 2^31-1 (2 GiB - 1), but we will sanitize this value to 500 MiB.
    data_stream_window_size: NonZeroByteCount,
}

impl NetworkingOptions {
    pub fn stream_window_size(&self) -> u32 {
        // santize to 500MiB if set higher
        let stream_window_size = self.data_stream_window_size.as_u64().min(500 * 1024 * 1024); // Sanitize to 500MiB if set higher.

        u32::try_from(stream_window_size).expect("window size too big")
    }

    pub fn connection_window_size(&self) -> u32 {
        self.stream_window_size() * 3
    }
}

impl Default for NetworkingOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3).into(),
            connect_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(250),
                2.0,
                Some(10),
                Some(Duration::from_millis(3000)),
            ),
            handshake_timeout: Duration::from_secs(3).into(),
            http2_keep_alive_interval: Duration::from_secs(1).into(),
            http2_keep_alive_timeout: Duration::from_secs(3).into(),
            http2_adaptive_window: true,
            disable_compression: false,
            // 2MiB
            data_stream_window_size: NonZeroByteCount::new(
                NonZeroUsize::new(2 * 1024 * 1024).expect("Non zero number"),
            ),
        }
    }
}
