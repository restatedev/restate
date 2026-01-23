// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::proxy::ProxyConnector;

use crate::utils::ErrorExt;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::Either;
use http::Version;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::http::HeaderValue;
use hyper::http::uri::PathAndQuery;
use hyper::{HeaderMap, Method, Request, Response, Uri};
use hyper_rustls::{ConfigBuilderExt, HttpsConnector};
use hyper_util::client::legacy::connect::HttpConnector;
use restate_types::config::HttpOptions;
use rustls::ClientConfig;
use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, LazyLock};
use std::{fmt, future};

type ProxiedHttpsConnector = ProxyConnector<HttpsConnector<HttpConnector>>;

static TLS_CLIENT_CONFIG: LazyLock<ClientConfig> = LazyLock::new(|| {
    // We need to explicitly configure the crypto provider since we activate the ring as well as
    // aws_lc_rs rustls feature, and they are mutually exclusive wrt auto installation. Moreover,
    // we don't want that tests need to install a crypto provider when using the HttpClient
    ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .expect("default versions are supported")
        .with_native_roots()
        .expect("Can load native certificates")
        .with_no_client_auth()
});

// TODO
//  for the time being we use BoxBody here to simplify the migration to hyper 1.0.
//  We should consider replacing this with some concrete type that makes sense.
type BoxError = Box<dyn Error + Send + Sync + 'static>;
type BoxBody = http_body_util::combinators::BoxBody<Bytes, BoxError>;

#[derive(Clone, Debug)]
pub struct HttpClient {
    /// Client used for HTTPS as long as HTTP1.1 or HTTP2 was not specifically requested.
    /// All HTTP versions are possible.
    alpn_client: hyper_util::client::legacy::Client<ProxiedHttpsConnector, BoxBody>,

    /// Client when HTTP1.1 was specifically requested - even if the ALPN advertises
    /// h2, we will not use it.
    h1_client: hyper_util::client::legacy::Client<ProxiedHttpsConnector, BoxBody>,

    /// Client when HTTP2 was specifically requested - for cleartext, we use h2c,
    /// and for HTTPS, we will fail unless the ALPN supports h2.
    /// In practice, at discovery time we never force h2 for HTTPS.
    h2_client: hyper_util::client::legacy::Client<ProxiedHttpsConnector, BoxBody>,
}

impl HttpClient {
    pub fn from_options(options: &HttpOptions) -> HttpClient {
        let mut builder =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::default());
        builder.timer(hyper_util::rt::TokioTimer::default());

        builder
            .http2_initial_max_send_streams(options.initial_max_send_streams)
            .http2_adaptive_window(true)
            .http2_keep_alive_timeout(options.http_keep_alive_options.timeout.into())
            .http2_keep_alive_interval(Some(options.http_keep_alive_options.interval.into()));

        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        http_connector.set_nodelay(true);
        http_connector.set_connect_timeout(Some(options.connect_timeout.into()));

        let https_alpn_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(TLS_CLIENT_CONFIG.clone())
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(http_connector.clone());

        let https_h1_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(TLS_CLIENT_CONFIG.clone())
            .https_or_http()
            .enable_http1()
            .wrap_connector(http_connector.clone());

        let https_h2_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(TLS_CLIENT_CONFIG.clone())
            .https_or_http()
            .enable_http2()
            .wrap_connector(http_connector.clone());

        HttpClient {
            alpn_client: builder.clone().build::<_, BoxBody>(ProxyConnector::new(
                options.http_proxy.clone(),
                options.no_proxy.clone(),
                https_alpn_connector,
            )),
            h1_client: builder.clone().build::<_, BoxBody>(ProxyConnector::new(
                options.http_proxy.clone(),
                options.no_proxy.clone(),
                https_h1_connector,
            )),
            h2_client: {
                builder.http2_only(true);
                builder.build::<_, BoxBody>(ProxyConnector::new(
                    options.http_proxy.clone(),
                    options.no_proxy.clone(),
                    https_h2_connector,
                ))
            },
        }
    }

    fn build_request<B>(
        uri: Uri,
        version: Option<Version>,
        body: B,
        method: Method,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> Result<Request<BoxBody>, http::Error>
    where
        B: Body<Data = Bytes> + Send + Sync + Unpin + Sized + 'static,
        <B as Body>::Error: Error + Send + Sync + 'static,
    {
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
                    format!("{path}?{query}")
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

        if let Some(version) = version {
            http_request_builder = http_request_builder.version(version);
        }

        http_request_builder.body(BoxBody::new(body.map_err(|e| e.into())))
    }

    pub fn request<B>(
        &self,
        uri: Uri,
        version: Option<Version>,
        method: Method,
        body: B,
        path: PathAndQuery,
        headers: HeaderMap<HeaderValue>,
    ) -> impl Future<Output = Result<Response<hyper::body::Incoming>, HttpError>> + Send + 'static
    where
        B: Body<Data = Bytes> + Send + Sync + Unpin + Sized + 'static,
        <B as Body>::Error: Error + Send + Sync + 'static,
    {
        let request = match Self::build_request(uri, version, body, method, path, headers) {
            Ok(request) => request,
            Err(err) => return future::ready(Err(err.into())).right_future(),
        };

        let fut = match version {
            // version is set to http1.1 when use_http1.1 is set
            Some(Version::HTTP_11) => self.h1_client.request(request),
            // version is set to http2 for cleartext urls when use_http1.1 is not set
            Some(Version::HTTP_2) => self.h2_client.request(request),
            // version is currently set to none for https urls when use_http1.1 is not set
            None => self.alpn_client.request(request),
            // nothing currently sets a different version, but the alpn client is a sensible default
            Some(_) => self.alpn_client.request(request),
        };

        Either::Left(async move {
            match fut.await {
                Ok(res) => Ok(res),
                Err(err) => Err(err.into()),
            }
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error(transparent)]
    Http(#[from] http::Error),
    #[error("server possibly supports only HTTP1.1, consider discovery with --use-http1.1.\nReason: {}", FormatHyperError(.0))]
    PossibleHTTP11Only(#[source] hyper_util::client::legacy::Error),
    #[error("server possibly supports only HTTP/2, consider discovering without --use-http1.1.\nReason: {}", FormatHyperError(.0))]
    PossibleHTTP2Only(#[source] hyper_util::client::legacy::Error),
    #[error("unable to reach the remote endpoint.\nReason: {}", FormatHyperError(.0))]
    Connect(#[source] hyper_util::client::legacy::Error),
    #[error("{}", FormatHyperError(.0))]
    Hyper(#[source] hyper_util::client::legacy::Error),
}

impl HttpError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            HttpError::Hyper(err) => err.is_retryable(),
            HttpError::Http(err) => err.is_retryable(),
            HttpError::PossibleHTTP11Only(_) => false,
            HttpError::PossibleHTTP2Only(_) => false,
            HttpError::Connect(_) => true,
        }
    }

    fn is_possible_h11_only_error(err: &hyper_util::client::legacy::Error) -> bool {
        // this is the error we see from the h2 lib when the server sends back an http1.1 response
        // to an http2 request. http2 is designed to start requests with what looks like an invalid
        // HTTP1.1 method, so typically 1.1 servers respond with a 40x, and the h2 client sees
        // this as an invalid frame.
        err.source()
            // Cause can either be h2 directly, or hyper::Error and then h2.
            .and_then(|err| {
                err.downcast_ref::<h2::Error>().or_else(|| {
                    err.downcast_ref::<hyper::Error>()
                        .and_then(|e| e.source())
                        .and_then(|err| err.downcast_ref::<h2::Error>())
                })
            })
            .and_then(|err| err.reason())
            == Some(h2::Reason::FRAME_SIZE_ERROR)
    }

    fn is_possible_h2_only_error(err: &hyper_util::client::legacy::Error) -> bool {
        // This is a reasonably fuzzy check to figure out if the user passed --http1.1-only when doing discovery,
        // but the service supports only HTTP/2
        err.source()
            .and_then(|err| err.downcast_ref::<hyper::Error>())
            .map(|err| {
                use std::fmt::Write;

                // Write dance to avoid allocating strings for matching the specific error string below.
                //  Unfortunately there's no other way to match this error from hyper APIs :(
                struct Matcher(bool);
                impl Write for Matcher {
                    fn write_str(&mut self, s: &str) -> fmt::Result {
                        if s == "invalid HTTP version parsed" {
                            self.0 = true;
                        }
                        Ok(())
                    }
                }

                let mut matcher = Matcher(false);
                let _ = write!(&mut matcher, "{err}");
                matcher.0
            })
            .unwrap_or(false)
    }
}

impl From<hyper_util::client::legacy::Error> for HttpError {
    fn from(err: hyper_util::client::legacy::Error) -> Self {
        if Self::is_possible_h11_only_error(&err) {
            Self::PossibleHTTP11Only(err)
        } else if Self::is_possible_h2_only_error(&err) {
            Self::PossibleHTTP2Only(err)
        } else if err.is_connect() {
            Self::Connect(err)
        } else {
            Self::Hyper(err)
        }
    }
}

struct FormatHyperError<'a>(&'a hyper_util::client::legacy::Error);

impl fmt::Display for FormatHyperError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(err) = source {
            write!(f, " caused by: {err}")?;
            source = err.source();
        }

        Ok(())
    }
}
