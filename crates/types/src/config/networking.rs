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
use std::time::Duration;

use restate_util_bytecount::NonZeroByteCount;
use restate_util_time::NonZeroFriendlyDuration;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::retries::RetryPolicy;

/// The default maximum size for messages (32 MiB).
pub const DEFAULT_MESSAGE_SIZE_LIMIT: NonZeroUsize = NonZeroUsize::new(32 * 1024 * 1024).unwrap();
pub const DEFAULT_FABRIC_MEMORY_LIMIT: NonZeroUsize = NonZeroUsize::new(64 * 1024 * 1024).unwrap();

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
    pub connect_timeout: NonZeroFriendlyDuration,

    /// # Connect retry policy
    ///
    /// Retry policy to use for internal node-to-node networking.
    pub connect_retry_policy: RetryPolicy,

    /// # Handshake timeout
    ///
    /// Timeout for receiving a handshake response from Restate cluster peers.
    pub handshake_timeout: NonZeroFriendlyDuration,

    /// # HTTP/2 Keep Alive Interval
    ///
    /// Interval at which HTTP/2 PING frames are sent on node-to-node
    /// connections, to keep them alive and to detect peers that have become
    /// unreachable.
    ///
    /// Applies both to the gRPC channels a node opens to its peers and to the
    /// connections it accepts from them.
    pub http2_keep_alive_interval: NonZeroFriendlyDuration,

    /// # HTTP/2 Keep Alive Timeout
    ///
    /// How long to wait for a peer to acknowledge a keep-alive PING on a
    /// node-to-node connection. If the acknowledgement does not arrive within
    /// this timeout, the connection is closed and re-established.
    pub http2_keep_alive_timeout: NonZeroFriendlyDuration,

    /// # HTTP/2 Adaptive Window
    ///
    /// Deprecated since v1.8.0 and has no effect. Node-to-node connections use fixed HTTP/2
    /// flow-control windows sized by `data-stream-window-size`.
    #[deprecated(since = "1.8.0", note = "Has no effect; use `data-stream-window-size`")]
    #[serde(skip_serializing_if = "Option::is_none")]
    http2_adaptive_window: Option<bool>,

    /// # Disable Compression
    ///
    /// Disables Zstd compression for internal gRPC network connections
    pub disable_compression: bool,

    /// # Data Stream Window Size
    ///
    /// Controls how many bytes a node can send to another node before the receiving node has
    /// processed them. Beyond that, the sender waits, which applies back pressure.
    ///
    /// The value is best derived from the bandwidth-delay product (BDP) of the network. For
    /// instance, if the network has a bandwidth of 10 Gbps and a round-trip time of 5 ms, the BDP
    /// is 10 Gbps * 0.005 s = 6.25 MB. The window should be at least the BDP to fully utilize the
    /// bandwidth, assuming the latency is constant. We recommend twice the BDP to account for
    /// variations in latency.
    ///
    /// At 10 Gbps, the default of 4 MiB is twice the BDP of a round trip of about 1.7 ms, which
    /// covers typical networks within a data center. On high-latency links, the window limits
    /// throughput to about one window per round trip, for example about 40 MiB/s at 100 ms, so
    /// raise it there. Each connection can buffer up to one window of data that the receiving
    /// node has not processed yet.
    ///
    /// Windows above 4 MiB also need larger TCP buffers on every node, because the kernel limits
    /// each connection's buffers independently of this setting. On Linux, raise the maximum
    /// (third) values of `net.ipv4.tcp_wmem` and `net.ipv4.tcp_rmem`.
    ///
    /// The maximum theoretical value is 2^31-1 (2 GiB - 1), but values above 500 MiB are reduced
    /// to 500 MiB. Since v1.8.0, the default is 4 MiB (previously 2 MiB).
    data_stream_window_size: NonZeroByteCount,

    /// # Networking Message Size Limit
    ///
    /// Maximum size of a message that can be sent or received over the network.
    /// This applies to communication between Restate cluster nodes, as well as
    /// between Restate servers and external tools such as CLI and management APIs.
    ///
    /// Default: `32MiB`
    #[serde(
        default = "default_message_size_limit",
        skip_serializing_if = "is_default_message_size_limit"
    )]
    pub message_size_limit: NonZeroByteCount,

    /// # Global Fabric Memory Limit
    ///
    /// This sets the memory limit for all in-flight fabric services that don't own dedicated
    /// memory pools. The memory limit will be sanitized to the configured `message-size-limit`
    /// if smaller.
    ///
    /// Default: `64MiB`
    #[serde(
        default = "default_fabric_memory_limit",
        skip_serializing_if = "is_default_fabric_memory_limit"
    )]
    fabric_memory_limit: NonZeroByteCount,
}

const fn default_message_size_limit() -> NonZeroByteCount {
    NonZeroByteCount::new(DEFAULT_MESSAGE_SIZE_LIMIT)
}

fn is_default_message_size_limit(value: &NonZeroByteCount) -> bool {
    value.as_non_zero_usize() == DEFAULT_MESSAGE_SIZE_LIMIT
}

const fn default_fabric_memory_limit() -> NonZeroByteCount {
    NonZeroByteCount::new(DEFAULT_FABRIC_MEMORY_LIMIT)
}

fn is_default_fabric_memory_limit(value: &NonZeroByteCount) -> bool {
    value.as_non_zero_usize() == DEFAULT_FABRIC_MEMORY_LIMIT
}

impl NetworkingOptions {
    pub fn stream_window_size(&self) -> u32 {
        // sanitize to 500MiB if set higher
        let stream_window_size = self.data_stream_window_size.as_u64().min(500 * 1024 * 1024); // Sanitize to 500MiB if set higher.

        u32::try_from(stream_window_size).expect("window size too big")
    }

    pub fn connection_window_size(&self) -> u32 {
        self.stream_window_size()
    }

    pub fn fabric_memory_limit(&self) -> NonZeroByteCount {
        self.fabric_memory_limit.max(self.message_size_limit)
    }
}

impl Default for NetworkingOptions {
    fn default() -> Self {
        #[allow(deprecated)]
        Self {
            connect_timeout: NonZeroFriendlyDuration::from_secs_unchecked(3),
            connect_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(250),
                2.0,
                Some(10),
                Some(Duration::from_millis(3000)),
            ),
            handshake_timeout: NonZeroFriendlyDuration::from_secs_unchecked(3),
            http2_keep_alive_interval: NonZeroFriendlyDuration::from_secs_unchecked(1),
            http2_keep_alive_timeout: NonZeroFriendlyDuration::from_secs_unchecked(3),
            http2_adaptive_window: None,
            disable_compression: false,
            // 4MiB
            data_stream_window_size: NonZeroByteCount::new(
                NonZeroUsize::new(4 * 1024 * 1024).expect("Non zero number"),
            ),
            message_size_limit: default_message_size_limit(),
            fabric_memory_limit: default_fabric_memory_limit(),
        }
    }
}
