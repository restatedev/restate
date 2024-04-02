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

use crate::request_signing;
use crate::request_signing::SignRequest;
use crate::utils::ErrorExt;
use arc_swap::ArcSwap;
use futures::future::Either;
use futures::FutureExt;
use hyper::client::HttpConnector;
use hyper::http::uri::PathAndQuery;
use hyper::http::HeaderValue;
use hyper::{Body, HeaderMap, Method, Request, Response, Uri, Version};
use hyper_rustls::HttpsConnector;
use serde_with::serde_as;
use std::fmt::Debug;
use std::future;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io;

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
    http_keep_alive_options: Option<Http2KeepAliveOptions>,
    /// # Proxy URI
    ///
    /// A URI, such as `http://127.0.0.1:10001`, of a server to which all invocations should be sent, with the `Host` header set to the deployment URI.
    /// HTTPS proxy URIs are supported, but only HTTP endpoint traffic will be proxied currently.
    /// Can be overridden by the `HTTP_PROXY` environment variable.
    #[cfg_attr(feature = "options_schema", schemars(with = "Option<String>"))]
    http_proxy: Option<Proxy>,

    /// # Request signing private key PEM file
    ///
    /// A path to a file, such as "/var/secrets/key.pem", which contains one or more ed25519 private
    /// keys in PEM format. Such a file can be generated with `openssl genpkey -algorithm ed25519`.
    /// If provided, these keys will be used to sign HTTP requests from this client in a custom format
    /// that SDKs may optionally verify, proving that the caller is a particular Restate instance.
    ///
    /// This file is currently only read on client creation, but this may change in future.
    /// Parsed public keys will be logged at INFO level in the same format that SDKs expect.
    request_signing_private_key_pem_file: Option<PathBuf>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            http_keep_alive_options: Some(Default::default()),
            http_proxy: None,
            request_signing_private_key_pem_file: None,
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

type Connector = ProxyConnector<HttpsConnector<HttpConnector>>;

#[derive(Clone, Debug)]
pub struct HttpClient {
    client: hyper::Client<Connector, Body>,
    // this can be changed to re-read periodically if necessary
    request_signing_keys: Arc<ArcSwap<Vec<request_signing::v1::SigningKey>>>,
}

impl HttpClient {
    pub fn new(
        client: hyper::Client<Connector, Body>,
        request_signing_private_key_pem_file: Option<PathBuf>,
    ) -> Result<Self, HttpClientError> {
        let request_signing_keys = if let Some(request_signing_private_key_pem_file) =
            request_signing_private_key_pem_file
        {
            Arc::new(ArcSwap::new(Arc::new(request_signing::v1::read_pem_file(
                request_signing_private_key_pem_file,
            )?)))
        } else {
            Arc::new(ArcSwap::new(Arc::new(Vec::new())))
        };

        Ok(Self {
            client,
            request_signing_keys,
        })
    }

    pub fn from_options(options: Options) -> Result<HttpClient, BuildError> {
        let mut builder = hyper::Client::builder();
        builder.http2_only(true);

        if let Some(keep_alive_options) = options.http_keep_alive_options {
            builder
                .http2_keep_alive_timeout(keep_alive_options.timeout.into())
                .http2_keep_alive_interval(Some(keep_alive_options.interval.into()));
        }

        Ok(HttpClient::new(
            builder.build::<_, hyper::Body>(ProxyConnector::new(
                options.http_proxy,
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_or_http()
                    .enable_http2()
                    .build(),
            )),
            options.request_signing_private_key_pem_file,
        )?)
    }

    fn build_request(
        uri: Uri,
        version: Version,
        body: Body,
        method: Method,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> Result<Request<Body>, hyper::http::Error> {
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
            .method(method)
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
        body: Body,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> impl Future<Output = Result<Response<Body>, HttpError>> + Send + 'static {
        let method = Method::POST;

        let request_signing_keys = self.request_signing_keys.load();

        let signer = if !request_signing_keys.is_empty() {
            match request_signing::v1::Signer::new(
                method.as_str(),
                path.path(),
                request_signing_keys.as_slice(),
            ) {
                Ok(signing_parameters_v1) => Some(signing_parameters_v1),
                Err(err) => return future::ready(Err(err)).right_future(),
            }
        } else {
            None // will use null signing scheme
        };

        let request = match Self::build_request(uri, version, body, method, path, headers) {
            Ok(request) => request,
            Err(err) => return future::ready(Err(err.into())).right_future(),
        };

        let request = signer.sign_request(request);

        let fut = self.client.request(request);

        Either::Left(async move { Ok(fut.await?) })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error(transparent)]
    HttpClient(#[from] HttpClientError),
}

#[derive(Debug, thiserror::Error)]
pub enum HttpClientError {
    #[error("Failed to read request signing private key: {0}")]
    SigningPrivateKeyReadError(#[from] SigningPrivateKeyReadError),
}

#[derive(Debug, thiserror::Error)]
pub enum SigningPrivateKeyReadError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Pem(#[from] pem::PemError),
    #[error("Key was rejected by ring: {0}")]
    KeyRejected(ring::error::KeyRejected),
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    Http(#[from] hyper::http::Error),
}

impl HttpError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            HttpError::Hyper(err) => err.is_retryable(),
            HttpError::Http(err) => err.is_retryable(),
        }
    }
}
