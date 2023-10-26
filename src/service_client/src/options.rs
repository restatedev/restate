// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use serde_with::serde_as;

use crate::proxy::{Proxy, ProxyConnector};
use crate::{Connector, ServiceClient};

pub use crate::lambda::{
    Options as LambdaClientOptions, OptionsBuilder as LambdaClientOptionsBuilder,
    OptionsBuilderError as LambdaClientOptionsBuilderError,
};

/// # Client options
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "ServiceClientOptions", default)
)]
#[builder(default)]
#[derive(Default)]
pub struct Options {
    http: HttpClientOptions,
    lambda: LambdaClientOptions,
}

impl Options {
    pub fn build(self) -> ServiceClient<Connector, hyper::Body> {
        ServiceClient::new(self.http.build(), self.lambda.build())
    }
}

/// # HTTP client options
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(default))]
#[builder(default)]
pub struct HttpClientOptions {
    /// # HTTP/2 Keep-alive
    ///
    /// Configuration for the HTTP/2 keep-alive mechanism, using PING frames.
    /// If unset, HTTP/2 keep-alive are disabled.
    keep_alive_options: Option<Http2KeepAliveOptions>,
    /// # Proxy URI
    ///
    /// A URI, such as `http://127.0.0.1:10001`, of a server to which all invocations should be sent, with the `Host` header set to the service endpoint URI.
    /// HTTPS proxy URIs are supported, but only HTTP endpoint traffic will be proxied currently.
    /// Can be overridden by the `HTTP_PROXY` environment variable.
    #[cfg_attr(feature = "options_schema", schemars(with = "Option<String>"))]
    proxy_uri: Option<Proxy>,
}

impl Default for HttpClientOptions {
    fn default() -> Self {
        Self {
            keep_alive_options: Some(Default::default()),
            proxy_uri: None,
        }
    }
}

impl HttpClientOptions {
    pub fn build(self) -> hyper::Client<Connector, hyper::Body> {
        let mut builder = hyper::Client::builder();
        builder.http2_only(true);

        if let Some(keep_alive_options) = self.keep_alive_options {
            builder
                .http2_keep_alive_timeout(keep_alive_options.timeout.into())
                .http2_keep_alive_interval(Some(keep_alive_options.interval.into()));
        }

        builder.build::<_, hyper::Body>(ProxyConnector::new(
            self.proxy_uri,
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                .enable_http2()
                .build(),
        ))
    }
}

/// # HTTP/2 Keep alive options
///
/// Configuration for the HTTP/2 keep-alive mechanism, using PING frames.
///
/// Please note: most gateways don't propagate the HTTP/2 keep-alive between downstream and upstream hosts.
/// In those environments, you need to make sure the gateway can detect a broken connection to the upstream service endpoint(s).
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(default))]
pub struct Http2KeepAliveOptions {
    /// # HTTP/2 Keep-alive interval
    ///
    /// Sets an interval for HTTP/2 PING frames should be sent to keep a
    /// connection alive.
    ///
    /// You should set this timeout with a value lower than the `response_abort_timeout`.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    pub(crate) interval: humantime::Duration,

    /// # Timeout
    ///
    /// Sets a timeout for receiving an acknowledgement of the keep-alive ping.
    ///
    /// If the ping is not acknowledged within the timeout, the connection will
    /// be closed.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    pub(crate) timeout: humantime::Duration,
}

impl Default for Http2KeepAliveOptions {
    fn default() -> Self {
        Self {
            interval: Http2KeepAliveOptions::default_interval(),
            timeout: Http2KeepAliveOptions::default_timeout(),
        }
    }
}

impl Http2KeepAliveOptions {
    fn default_interval() -> humantime::Duration {
        (Duration::from_secs(40)).into()
    }

    fn default_timeout() -> humantime::Duration {
        (Duration::from_secs(20)).into()
    }
}
