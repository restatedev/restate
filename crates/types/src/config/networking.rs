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
use std::time::Duration;

use restate_serde_util::NonZeroByteCount;
use restate_time_util::NonZeroFriendlyDuration;

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
    pub http2_keep_alive_interval: NonZeroFriendlyDuration,

    /// # HTTP/2 Keep Alive Timeout
    pub http2_keep_alive_timeout: NonZeroFriendlyDuration,

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

    /// # TLS Configuration
    ///
    /// Optional TLS/mTLS configuration for inter-node fabric communication.
    /// When set, the fabric port uses TLS for both inbound and outbound connections.
    /// Without this section, fabric communication remains plaintext (default behavior).
    ///
    /// Since v1.3.0
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<FabricTlsOptions>,
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
        // santize to 500MiB if set higher
        let stream_window_size = self.data_stream_window_size.as_u64().min(500 * 1024 * 1024); // Sanitize to 500MiB if set higher.

        u32::try_from(stream_window_size).expect("window size too big")
    }

    pub fn connection_window_size(&self) -> u32 {
        self.stream_window_size() * 3
    }

    pub fn fabric_memory_limit(&self) -> NonZeroByteCount {
        self.fabric_memory_limit.max(self.message_size_limit)
    }
}

impl Default for NetworkingOptions {
    fn default() -> Self {
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
            http2_adaptive_window: true,
            disable_compression: false,
            // 2MiB
            data_stream_window_size: NonZeroByteCount::new(
                NonZeroUsize::new(2 * 1024 * 1024).expect("Non zero number"),
            ),
            message_size_limit: default_message_size_limit(),
            fabric_memory_limit: default_fabric_memory_limit(),
            tls: None,
        }
    }
}

/// TLS mode for fabric inter-node communication.
///
/// Since v1.3.0
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    /// Only TLS connections are accepted; plaintext is rejected.
    #[default]
    Strict,
    /// Both TLS and plaintext connections are accepted. Use during rolling upgrades.
    Optional,
}

/// TLS configuration for fabric inter-node communication.
///
/// Since v1.3.0
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub struct FabricTlsOptions {
    /// TLS enforcement mode. Default: `strict`.
    #[serde(default)]
    pub mode: TlsMode,

    /// Path to the PEM-encoded server certificate.
    pub cert_file: PathBuf,

    /// Path to the PEM-encoded private key.
    pub key_file: PathBuf,

    /// Paths to PEM-encoded CA certificates for verifying peer certificates.
    pub ca_files: Vec<PathBuf>,

    /// Require clients to present a valid certificate (mTLS). Default: `true`.
    #[serde(default = "default_require_client_auth")]
    pub require_client_auth: bool,

    /// How often to reload certificates from disk. Default: `1h`.
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval: NonZeroFriendlyDuration,

    /// Optional separate TLS configuration for outbound connections to peer nodes.
    /// If omitted, the server cert/key/ca are used for outbound connections as well.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<FabricTlsClientOptions>,
}

/// Separate client TLS config for outbound fabric connections.
/// Fields that are `None` inherit from the parent [`FabricTlsOptions`].
///
/// Since v1.3.0
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub struct FabricTlsClientOptions {
    /// Client certificate for outbound connections. Inherits from parent if omitted.
    pub cert_file: Option<PathBuf>,

    /// Client private key for outbound connections. Inherits from parent if omitted.
    pub key_file: Option<PathBuf>,

    /// Root CA files for verifying server certificates. Inherits from parent if omitted.
    pub root_ca_files: Option<Vec<PathBuf>>,
}

impl FabricTlsOptions {
    pub fn client_cert_file(&self) -> &PathBuf {
        self.client
            .as_ref()
            .and_then(|c| c.cert_file.as_ref())
            .unwrap_or(&self.cert_file)
    }

    pub fn client_key_file(&self) -> &PathBuf {
        self.client
            .as_ref()
            .and_then(|c| c.key_file.as_ref())
            .unwrap_or(&self.key_file)
    }

    pub fn client_ca_files(&self) -> &[PathBuf] {
        self.client
            .as_ref()
            .and_then(|c| c.root_ca_files.as_deref())
            .unwrap_or(&self.ca_files)
    }

    pub fn is_strict(&self) -> bool {
        self.mode == TlsMode::Strict
    }
}

fn default_require_client_auth() -> bool {
    true
}

fn default_refresh_interval() -> NonZeroFriendlyDuration {
    NonZeroFriendlyDuration::from_secs_unchecked(3600)
}
