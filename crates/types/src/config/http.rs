// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;

use http::Uri;
use http::uri::{InvalidUri, Parts, Scheme};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_serde_util::authority::AuthoritySerde;
use restate_time_util::NonZeroFriendlyDuration;

/// # HTTP client options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "HttpClientOptions", default))]
#[builder(default)]
#[serde(rename_all = "kebab-case")]
pub struct HttpOptions {
    /// # HTTP/2 Keep-alive
    ///
    /// Configuration for the HTTP/2 keep-alive mechanism, using PING frames.
    /// If unset, HTTP/2 keep-alive are disabled.
    pub http_keep_alive_options: Http2KeepAliveOptions,
    /// # Proxy URI
    ///
    /// A URI, such as `http://127.0.0.1:10001`, of a server to which all invocations should be sent, with the `Host` header set to the deployment URI.
    /// HTTPS proxy URIs are supported, but only HTTP endpoint traffic will be proxied currently.
    /// Can be overridden by the `HTTP_PROXY` environment variable.
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub http_proxy: Option<ProxyUri>,

    /// # No proxy
    ///
    /// HTTP authorities eg `localhost`, `restate.dev`, `127.0.0.1` that should not be proxied by the http_proxy.
    /// Ports are ignored. Subdomains are also matched. An entry “*” matches all hostnames.
    /// Can be overridden by the `NO_PROXY` environment variable, which supports comma separated values.
    #[serde_as(as = "Vec<AuthoritySerde>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub no_proxy: Vec<http::uri::Authority>,

    /// # Connect timeout
    ///
    /// How long to wait for a TCP connection to be established before considering
    /// it a failed attempt.
    pub connect_timeout: NonZeroFriendlyDuration,

    /// # Initial Max Send Streams
    ///
    /// Sets the initial maximum of locally initiated (send) streams.
    ///
    /// This value will be overwritten by the value included in the initial
    /// SETTINGS frame received from the peer as part of a [connection preface].
    ///
    /// Default: None
    ///
    /// **NOTE**: Setting this value to None (default) users the default
    /// recommended value from HTTP2 specs
    pub initial_max_send_streams: Option<usize>,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            http_keep_alive_options: Http2KeepAliveOptions::default(),
            http_proxy: None,
            no_proxy: Vec::new(),
            connect_timeout: NonZeroFriendlyDuration::from_secs_unchecked(10),
            initial_max_send_streams: None,
        }
    }
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
    /// You should set this timeout with a value lower than the `abort_timeout`.
    pub interval: NonZeroFriendlyDuration,

    /// # Timeout
    ///
    /// Sets a timeout for receiving an acknowledgement of the keep-alive ping.
    ///
    /// If the ping is not acknowledged within the timeout, the connection will
    /// be closed.
    pub timeout: NonZeroFriendlyDuration,
}

impl Default for Http2KeepAliveOptions {
    fn default() -> Self {
        Self {
            interval: NonZeroFriendlyDuration::from_secs_unchecked(40),
            timeout: NonZeroFriendlyDuration::from_secs_unchecked(20),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("invalid proxy Uri (must have scheme, authority, and path): {0}")]
pub struct InvalidProxyUri(Uri);

#[derive(Clone, Debug, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProxyUri {
    uri: Uri,
}

impl fmt::Display for ProxyUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.uri.fmt(f)
    }
}

impl TryFrom<String> for ProxyUri {
    type Error = ProxyFromStrError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        ProxyUri::from_str(&value)
    }
}

impl From<ProxyUri> for String {
    fn from(value: ProxyUri) -> Self {
        value.to_string()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyFromStrError {
    #[error(transparent)]
    InvalidUri(#[from] InvalidUri),
    #[error(transparent)]
    InvalidProxyUri(#[from] InvalidProxyUri),
}

impl FromStr for ProxyUri {
    type Err = ProxyFromStrError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(Uri::from_str(s)?)?)
    }
}

impl ProxyUri {
    pub fn new(proxy_uri: Uri) -> Result<Self, InvalidProxyUri> {
        match proxy_uri.clone().into_parts() {
            // all three must be present
            Parts {
                scheme: Some(_),
                authority: Some(_),
                path_and_query: Some(_),
                ..
            } => Ok(Self { uri: proxy_uri }),
            _ => Err(InvalidProxyUri(proxy_uri)),
        }
    }

    pub fn dst(&self, dst: Uri) -> Uri {
        // only proxy non TLS traffic, otherwise just pass through directly to underlying connector
        if dst.scheme() != Some(&Scheme::HTTPS) {
            let mut parts = self.clone().uri.into_parts();
            parts.path_and_query = dst.path_and_query().cloned();

            Uri::from_parts(parts).unwrap()
        } else {
            dst
        }
    }
}
