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

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_memory::NonZeroByteCount;
use restate_util_time::{FriendlyDuration, NonZeroFriendlyDuration};

/// # HTTP client options
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "HttpClientOptions", default))]
#[builder(default)]
#[serde(rename_all = "kebab-case")]
pub struct HttpOptions {
    /// # HTTP/2 Keep-alive
    ///
    /// Configuration for the HTTP/2 keep-alive mechanism, using PING frames.
    #[serde(flatten)]
    pub http_keep_alive_options: Http2KeepAliveOptions,
    /// # Proxy URI
    ///
    /// A URI, such as `http://127.0.0.1:10001`, of a server to which all invocations should be sent, with the `Host` header set to the deployment URI.
    /// HTTPS proxy URIs are supported, but only HTTP endpoint traffic will be proxied currently.
    /// Can be overridden by the `HTTP_PROXY` environment variable.
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub http_proxy: Option<String>,

    /// # No proxy
    ///
    /// IP subnets, addresses, and domain names eg `localhost,restate.dev,127.0.0.1,::1,192.168.1.0/24` that should not be proxied by the http_proxy.
    /// IP addresses must not have ports, and IPv6 addresses must not be wrapped in '[]'.
    /// Subdomains are also matched. An entry “*” matches all hostnames.
    /// Can be overridden by the `NO_PROXY` environment variable, which supports comma separated values.
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub no_proxy: Option<NoProxy>,

    /// # Connect timeout
    ///
    /// How long to wait for a TCP connection to be established before considering
    /// it a failed attempt.
    pub connect_timeout: NonZeroFriendlyDuration,

    /// # HTTP/2 Initial Max Send Streams
    ///
    /// Sets the initial maximum of locally initiated (send) streams.
    ///
    /// This value will be overwritten by the value included in the initial
    /// SETTINGS frame received from the peer as part of a [connection preface].
    ///
    /// Note: This value is capped by [`Self::streams_per_connection_limit`]
    ///
    /// Default: None
    ///
    /// **NOTE**: Setting this value to None (default) users the default
    /// recommended value from HTTP2 specs
    pub http2_initial_max_send_streams: Option<NonZeroU32>,

    /// Upper bound on the per-connection max-send-streams.
    ///
    /// Caps the remote server's advertised `max_concurrent_streams`.
    ///
    /// A high number of concurrent streams per connection works
    /// poorly with L4 load balancers because streams are not balanced across
    /// backends.
    ///
    /// Since v1.7.0
    ///
    /// Default: 128
    pub http2_streams_per_connection_limit: NonZeroUsize,

    /// # HTTP/2 Idle Connection Timeout
    ///
    /// How long a connection can be idle before it is evicted
    /// and closed. Set to `0` to disable eviction.
    ///
    /// Since: v1.7.0
    ///
    /// Default: 5 minutes
    pub http2_idle_connection_timeout: FriendlyDuration,

    /// # HTTP/2 initial stream window size
    ///
    /// Initial flow-control window (in bytes) for received data on each HTTP/2
    /// stream in the connection pool. Valid range: 65535 B .. 2 GiB.
    ///
    /// Since: v1.7.0
    ///
    /// Default: 2 MiB
    http2_initial_stream_window_size: NonZeroByteCount,

    /// # HTTP/2 initial connection window size
    ///
    /// Initial connection-level flow-control window (in bytes) for received
    /// data. Should be >= the per-stream window. Valid range: 65535 B .. 2 GiB.
    ///
    /// Since: v1.7.0
    ///
    /// Default: 5 MiB
    http2_initial_connection_window_size: NonZeroByteCount,

    /// # HTTP/2 max frame size
    ///
    /// Largest HTTP/2 DATA frame payload (in bytes) this client will accept.
    /// Must be within 16 KiB .. 16 MiB (h2 protocol limits).
    ///
    /// Since: v1.7.0
    ///
    /// Default: 16 KiB
    http2_max_frame_size: NonZeroByteCount,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            http_keep_alive_options: Http2KeepAliveOptions::default(),
            http_proxy: None,
            no_proxy: None,
            connect_timeout: NonZeroFriendlyDuration::from_secs_unchecked(10),
            http2_initial_max_send_streams: None,
            http2_streams_per_connection_limit: NonZeroUsize::new(128).unwrap(),
            http2_idle_connection_timeout: FriendlyDuration::from_secs(300),
            http2_initial_stream_window_size: NonZeroByteCount::new(
                NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
            ),
            http2_initial_connection_window_size: NonZeroByteCount::new(
                NonZeroUsize::new(5 * 1024 * 1024).unwrap(),
            ),
            http2_max_frame_size: NonZeroByteCount::new(NonZeroUsize::new(16 * 1024).unwrap()),
        }
    }
}

impl HttpOptions {
    pub fn initial_stream_window_size(&self) -> u32 {
        u32::try_from(self.http2_initial_stream_window_size.as_usize())
            .unwrap_or(u32::MAX)
            .clamp(65_535, 2_147_483_647)
    }

    pub fn initial_connection_window_size(&self) -> u32 {
        u32::try_from(self.http2_initial_connection_window_size.as_usize())
            .unwrap_or(u32::MAX)
            .clamp(65_535, 2_147_483_647)
    }

    pub fn max_frame_size(&self) -> u32 {
        u32::try_from(self.http2_max_frame_size.as_usize())
            .unwrap_or(u32::MAX)
            .clamp(16_384, 16_777_215)
    }

    pub(crate) fn apply_deprecated(&mut self, new_base: &str, deprecated: DeprecatedHttpOptions) {
        let DeprecatedHttpOptions {
            http_keep_alive_options,
            http_proxy,
            no_proxy,
            connect_timeout,
            initial_max_send_streams,
        } = deprecated;

        super::apply_deprecated_field(
            &mut self.http_keep_alive_options.http2_keep_alive_interval,
            http_keep_alive_options.interval,
            new_base,
            "http-keep-alive-options.interval",
            Some("http2-keep-alive-interval"),
        );

        super::apply_deprecated_field(
            &mut self.http_keep_alive_options.http2_keep_alive_timeout,
            http_keep_alive_options.timeout,
            new_base,
            "http-keep-alive-options.timeout",
            Some("http2-keep-alive-timeout"),
        );

        super::apply_deprecated_field(
            &mut self.http_keep_alive_options.http2_keep_alive_jitter,
            http_keep_alive_options.jitter,
            new_base,
            "http-keep-alive-options.jitter",
            Some("http2-keep-alive-jitter"),
        );

        super::apply_deprecated_field_optional(
            &mut self.http_proxy,
            http_proxy,
            new_base,
            "http-proxy",
            None,
        );

        super::apply_deprecated_field_optional(
            &mut self.no_proxy,
            no_proxy,
            new_base,
            "no-proxy",
            None,
        );

        super::apply_deprecated_field(
            &mut self.connect_timeout,
            connect_timeout,
            new_base,
            "connect-timeout",
            None,
        );
        super::apply_deprecated_field_optional(
            &mut self.http2_initial_max_send_streams,
            initial_max_send_streams,
            new_base,
            "initial-max-send-streams",
            Some("http2-initial-max-send-streams"),
        );
    }
}

/// Shadow of [`HttpOptions`] for the deprecated `service-client` root location. Every leaf field
/// is `Option<T>` so `None` means "user didn't set it" and `Some(_)` means "user set this value".
// todo: Remove in Restate v1.8
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub(crate) struct DeprecatedHttpOptions {
    pub http_keep_alive_options: DeprecatedHttp2KeepAliveOptions,
    pub http_proxy: Option<String>,
    pub no_proxy: Option<NoProxy>,
    pub connect_timeout: Option<NonZeroFriendlyDuration>,
    pub initial_max_send_streams: Option<NonZeroU32>,
}

/// Shadow of [`Http2KeepAliveOptions`] for the deprecated `service-client` root location.
// todo: Remove in Restate v1.8
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub(crate) struct DeprecatedHttp2KeepAliveOptions {
    pub interval: Option<FriendlyDuration>,
    pub timeout: Option<NonZeroFriendlyDuration>,
    pub jitter: Option<f32>,
}

/// NO_PROXY can be provided as either a comma-separated string `example.com,::1,localhost`, or a list of strings `["example.com", "::1", "localhost"]`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NoProxy {
    // no_proxy was an array pre 1.6, so for backwards compatibility we will continue to accept that
    List(Vec<String>),
    CommaSeparated(String),
}

/// # HTTP/2 Keep alive options
///
/// Configuration for the HTTP/2 keep-alive mechanism, using PING frames.
///
/// Please note: most gateways don't propagate the HTTP/2 keep-alive between downstream and upstream hosts.
/// In those environments, you need to make sure the gateway can detect a broken connection to the upstream deployment(s).
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(default))]
#[serde(rename_all = "kebab-case")]
pub struct Http2KeepAliveOptions {
    /// # HTTP/2 Keep-alive interval
    ///
    /// Sets an interval for HTTP/2 PING frames should be sent to keep a
    /// connection alive.
    ///
    /// `0` disables keep-alive pings entirely. Defaults to `40s`.
    ///
    /// You should set this timeout with a value lower than the `abort_timeout`.
    pub http2_keep_alive_interval: FriendlyDuration,

    /// # HTTP/2 Keep-Alive Timeout
    ///
    /// Sets a timeout for receiving an acknowledgement of the keep-alive ping.
    ///
    /// If the ping is not acknowledged within the timeout, the connection will
    /// be closed.
    ///
    /// Only meaningful when `http2-keep-alive-interval` is not zero. Defaults to 20 s.
    pub http2_keep_alive_timeout: NonZeroFriendlyDuration,

    /// # HTTP/2 Keep-Alive Jitter
    ///
    /// Fractional jitter added to `http2-keep-alive-interval`, expressed as a fraction
    /// of the interval (e.g. 0.1 = up to +10%, 1.0 = up to +100%).
    ///
    /// Default 0.2 (20% of http2-keep-alive-interval)
    pub http2_keep_alive_jitter: f32,
}

impl Default for Http2KeepAliveOptions {
    fn default() -> Self {
        Self {
            http2_keep_alive_interval: FriendlyDuration::from_secs(40),
            http2_keep_alive_timeout: NonZeroFriendlyDuration::from_secs_unchecked(20),
            http2_keep_alive_jitter: 0.2,
        }
    }
}
