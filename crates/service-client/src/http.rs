// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use futures::future::Either;
use futures::FutureExt;
use http::Version;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::http::uri::PathAndQuery;
use hyper::http::HeaderValue;
use hyper::{HeaderMap, Method, Request, Response, Uri};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::connect::HttpConnector;
use restate_types::config::HttpOptions;
use std::error::Error;
use std::fmt::Debug;
use std::future;
use std::future::Future;

type Connector = ProxyConnector<HttpsConnector<HttpConnector>>;

// TODO
//  for the time being we use BoxBody here to simplify the migration to hyper 1.0.
//  We should consider replacing this with some concrete type that makes sense.
type BoxError = Box<dyn Error + Send + Sync + 'static>;
type BoxBody = http_body_util::combinators::BoxBody<Bytes, BoxError>;

#[derive(Clone, Debug)]
pub struct HttpClient {
    // alpn client defaults to http1.1, but can upgrade to http2 using ALPN for TLS servers
    alpn_client: hyper_util::client::legacy::Client<Connector, BoxBody>,
    // h2 client defaults to http2 and so supports unencrypted http2 servers
    h2_client: hyper_util::client::legacy::Client<Connector, BoxBody>,
}

impl HttpClient {
    pub fn new(
        alpn_client: hyper_util::client::legacy::Client<Connector, BoxBody>,
        h2_client: hyper_util::client::legacy::Client<Connector, BoxBody>,
    ) -> Self {
        Self {
            alpn_client,
            h2_client,
        }
    }

    pub fn from_options(options: &HttpOptions) -> HttpClient {
        let mut builder =
            hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::default());
        builder.timer(hyper_util::rt::TokioTimer::default());

        builder
            .http2_keep_alive_timeout(options.http_keep_alive_options.timeout.into())
            .http2_keep_alive_interval(Some(options.http_keep_alive_options.interval.into()));

        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        http_connector.set_nodelay(true);
        http_connector.set_connect_timeout(Some(options.connect_timeout.into()));

        let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("Can build native roots")
            .https_or_http()
            .enable_http2()
            .wrap_connector(http_connector);

        let proxy_connector = ProxyConnector::new(options.http_proxy.clone(), https_connector);

        HttpClient::new(
            builder.clone().build::<_, BoxBody>(proxy_connector.clone()), // h1 client with alpn upgrade support
            {
                builder.http2_only(true);
                builder.build::<_, BoxBody>(proxy_connector) // h2-prior knowledge client
            },
        )
    }

    fn build_request<B>(
        uri: Uri,
        version: Version,
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

        http_request_builder.body(BoxBody::new(body.map_err(|e| e.into())))
    }

    pub fn request<B>(
        &self,
        uri: Uri,
        version: Version,
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

        let client = match request.version() {
            Version::HTTP_2 => &self.h2_client,
            _ => &self.alpn_client,
        };

        let fut = client.request(request);

        Either::Left(async move {
            match fut.await {
                Ok(res) => Ok(res),
                Err(err) if is_possible_h11_only_error(&err) => {
                    Err(HttpError::PossibleHTTP11Only(err))
                }
                Err(err) => Err(HttpError::Hyper(err)),
            }
        })
    }
}

fn is_possible_h11_only_error(err: &hyper_util::client::legacy::Error) -> bool {
    // this is the error we see from the h2 lib when the server sends back an http1.1 response
    // to an http2 request. http2 is designed to start requests with what looks like an invalid
    // HTTP1.1 method, so typically 1.1 servers respond with a 40x, and the h2 client sees
    // this as an invalid frame.
    err.source()
        .and_then(|err| err.downcast_ref::<h2::Error>())
        .and_then(|err| err.reason())
        == Some(h2::Reason::FRAME_SIZE_ERROR)
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error(transparent)]
    Hyper(#[from] hyper_util::client::legacy::Error),
    #[error(transparent)]
    Http(#[from] http::Error),
    #[error("server possibly only supports HTTP1.1, consider discovery with --use-http1.1: {0}")]
    PossibleHTTP11Only(#[source] hyper_util::client::legacy::Error),
}

impl HttpError {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            HttpError::Hyper(err) => err.is_retryable(),
            HttpError::Http(err) => err.is_retryable(),
            HttpError::PossibleHTTP11Only(_) => false,
        }
    }
}
