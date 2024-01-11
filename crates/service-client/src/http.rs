// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::proxy::{Proxy, ProxyConnector};

use futures::future::Either;
use hyper::body::Body;
use hyper::http::uri::PathAndQuery;
use hyper::http::HeaderValue;
use hyper::{http, HeaderMap, Method, Request, Response, Uri, Version};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_with::serde_as;
use std::fmt::Debug;
use std::future;
use std::future::Future;
use std::time::Duration;

/// # HTTP client options
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "HttpClientOptions", default)
)]
#[builder(default)]
pub struct Options {
    /// # HTTP/2 Keep-alive
    ///
    /// Configuration for the HTTP/2 keep-alive mechanism, using PING frames.
    /// If unset, HTTP/2 keep-alive are disabled.
    keep_alive_options: Option<Http2KeepAliveOptions>,
    /// # Proxy URI
    ///
    /// A URI, such as `http://127.0.0.1:10001`, of a server to which all invocations should be sent, with the `Host` header set to the deployment URI.
    /// HTTPS proxy URIs are supported, but only HTTP endpoint traffic will be proxied currently.
    /// Can be overridden by the `HTTP_PROXY` environment variable.
    #[cfg_attr(feature = "options_schema", schemars(with = "Option<String>"))]
    proxy_uri: Option<Proxy>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            keep_alive_options: Some(Default::default()),
            proxy_uri: None,
        }
    }
}

impl Options {
    pub fn build<B>(self) -> HttpClient<B> where
        B: Body + Send + Unpin + 'static,
        <B as Body>::Data: Send,
        <B as Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut builder = Client::builder(TokioExecutor::new());
        builder.http2_only(true);

        if let Some(keep_alive_options) = self.keep_alive_options {
            builder
                .http2_keep_alive_timeout(keep_alive_options.timeout.into())
                .http2_keep_alive_interval(Some(keep_alive_options.interval.into()));
        }

        //  builder.build::<_, hyper::Body>(ProxyConnector::new(
        //                 self.proxy_uri,
        //                 hyper_rustls::HttpsConnectorBuilder::new()
        //                     .with_native_roots()
        //                     .https_or_http()
        //                     .enable_http2()
        //                     .build(),
        //             )),

        HttpClient::new(
            builder.build::<_, B>(ProxyConnector::new(self.proxy_uri, HttpConnector::new())),
        )
    }
}

/// # HTTP/2 Keep alive options
///
/// Configuration for the HTTP/2 keep-alive mechanism, using PING frames.
///
/// Please note: most gateways don't propagate the HTTP/2 keep-alive between downstream and upstream hosts.
/// In those environments, you need to make sure the gateway can detect a broken connection to the upstream deployment(s).
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
    /// You should set this timeout with a value lower than the `abort_timeout`.
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
    #[inline]
    fn default_interval() -> humantime::Duration {
        (Duration::from_secs(40)).into()
    }

    #[inline]
    fn default_timeout() -> humantime::Duration {
        (Duration::from_secs(20)).into()
    }
}

type Connector = ProxyConnector<HttpConnector>;

#[derive(Clone, Debug)]
pub struct HttpClient<B> {
    client: Client<Connector, B>,
}

impl<B> HttpClient<B>
where
    B: Body + Send + Unpin + 'static,
    <B as Body>::Data: Send,
    <B as Body>::Error: std::error::Error + Send + Sync + 'static,
{
    pub fn new(client: Client<Connector, B>) -> Self {
        Self { client }
    }

    fn build_request(
        uri: Uri,
        version: Version,
        body: B,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> Result<Request<B>, hyper::http::Error> {
        let mut uri_parts = uri.into_parts();
        uri_parts.path_and_query = match uri_parts.path_and_query {
            None => Some(path),
            Some(existing_path) => Some({
                let path = format!(
                    "{}/{}",
                    existing_path
                        .path()
                        .strip_suffix('/')
                        .unwrap_or(existing_path.path()),
                    path.path().strip_prefix('/').unwrap_or(path.path()),
                );
                let path = if let Some(query) = existing_path.query() {
                    format!("{}?{}", path, query)
                } else {
                    path
                };

                path.try_into()?
            }),
        };

        let mut http_request_builder = Request::builder()
            .method(Method::POST)
            .uri(Uri::from_parts(uri_parts)?);

        for (header, value) in headers.iter() {
            http_request_builder = http_request_builder.header(header, value)
        }

        http_request_builder = http_request_builder.version(version);

        http_request_builder.body(body)
    }

    pub fn request(
        &self,
        uri: Uri,
        version: Version,
        body: B,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> impl Future<Output = Result<Response<hyper::body::Incoming>, HttpError>> + Send + 'static
    {
        let request = match Self::build_request(uri, version, body, path, headers) {
            Ok(request) => request,
            Err(err) => return Either::Right(future::ready(Err(err.into()))),
        };

        let fut = self.client.request(request);

        Either::Left(async move { Ok(fut.await?) })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    Http(#[from] http::Error),
    #[error(transparent)]
    LegacyClientError(#[from] hyper_util::client::legacy::Error),
}
