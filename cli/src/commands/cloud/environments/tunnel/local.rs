// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use http::{
    Uri,
    uri::{PathAndQuery, Scheme},
};
use hyper_rustls::ConfigBuilderExt;
use restate_cli_util::CliContext;
use restate_cloud_tunnel_client::client::{HandlerNotification, ServeError};
use restate_types::retries::RetryPolicy;
use rustls::ClientConfig;
use tokio_util::sync::CancellationToken;
use tower::Service;
use tracing::{Instrument, error, info, info_span};

use crate::clients::cloud::types::DescribeEnvironmentResponse;

use super::renderer::TunnelRenderer;

const HTTP_VERSION: http::HeaderName =
    http::HeaderName::from_static("x-restate-tunnel-http-version");

pub static TLS_CLIENT_CONFIG: LazyLock<ClientConfig> = LazyLock::new(|| {
    ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .expect("default versions are supported")
        .with_native_roots()
        .expect("Can load native certificates")
        .with_no_client_auth()
});

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_local(
    no_local: bool,
    alpn_client: reqwest::Client,
    h1_client: reqwest::Client,
    h2_client: reqwest::Client,
    bearer_token: &str,
    environment_info: DescribeEnvironmentResponse,
    tunnel_name: String,
    tunnel_renderer: Arc<TunnelRenderer>,
) -> Result<(), ServeError> {
    if no_local {
        return Ok(());
    }

    let resolver = hickory_resolver::Resolver::builder_tokio().unwrap().build();

    let tunnel_urls = resolver
        .srv_lookup(environment_info.tunnel_srv)
        .await
        .map_err(|err| ServeError::Connection(err.into()))?
        .iter()
        .map(|record| {
            Uri::builder()
                .scheme(Scheme::HTTPS)
                .path_and_query(PathAndQuery::from_static("/"))
                .authority(format!("{}:{}", record.target(), record.port()).as_str())
                .build()
                .map_err(|err| ServeError::Connection(err.into()))
        })
        .collect::<std::result::Result<Vec<Uri>, ServeError>>()?;

    tunnel_renderer.local.target_connected.store(
        tunnel_urls.len().min(64) as u8,
        std::sync::atomic::Ordering::Relaxed,
    );

    let mut http_connector = hyper_util::client::legacy::connect::HttpConnector::new();
    http_connector.set_nodelay(true);
    http_connector.set_connect_timeout(Some(CliContext::get().connect_timeout()));
    http_connector.enforce_http(false);
    // default interval on linux is 75 secs, also use this as the start-after
    http_connector.set_keepalive(Some(Duration::from_secs(75)));

    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(TLS_CLIENT_CONFIG.clone())
        .https_or_http()
        .enable_http2()
        .wrap_connector(http_connector);

    let mut futures: FuturesUnordered<_> = tunnel_urls.into_iter().enumerate().map(async |(tunnel_index, tunnel_url)| {
        let alpn_client = alpn_client.clone();
        let h1_client = h1_client.clone();
        let h2_client = h2_client.clone();

        let retry_policy = RetryPolicy::exponential(
            Duration::from_millis(10),
            2.0,
            None,
            Some(Duration::from_secs(12)),
        );

        let handler = restate_cloud_tunnel_client::client::Handler::<(), ()>::new(
            hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                do_proxy(&alpn_client, &h1_client, &h2_client, req)
            }),
            &environment_info.environment_id,
            &environment_info.signing_public_key,
            bearer_token,
            Some(tunnel_name.clone()),
            {
                let tunnel_renderer = tunnel_renderer.clone();
                Some(move |notification| match notification {
                    HandlerNotification::Started {
                        ..
                    } => {
                        tunnel_renderer.local.set_connected(tunnel_index, true);
                        tunnel_renderer.clear_error();
                    }
                    HandlerNotification::RequestIdentityError(err) => {
                        tunnel_renderer.store_error(format!("Request identity error, are you discovering from the right environment?\n  {err}"));
                    }
                    HandlerNotification::Error(err) => {
                        tunnel_renderer.store_error(err);
                    }
                    HandlerNotification::Request => {
                        tunnel_renderer.clear_error();
                    }
                })
            },
        )?;

        retry_policy.retry_if(
            || {
                let tunnel_url = tunnel_url.clone();
                let handler = handler.clone();
                let mut https_connector = https_connector.clone();

                async move {
                    let io = https_connector
                                .call(tunnel_url)
                                .await
                                .map_err(ServeError::Connection)?;

                    let err = handler
                        .serve(
                            io,
                            CancellationToken::new(),
                            CancellationToken::new(),
                        )
                        .await;

                    Err(err)
                }
            },
            |err: &ServeError| {
                if !err.is_retryable() {
                    return false;
                }

                tunnel_renderer.local.set_connected(tunnel_index, false);

                let err = match err.source() {
                    Some(source) => format!("{err}, retrying\n  Caused by: {source}"),
                    None => format!("{err}, retrying"),
                };
                tunnel_renderer.store_error(err);

                true
            },
        ).await
    }).collect();

    futures.next().await.unwrap()
}

fn do_proxy(
    alpn_client: &reqwest::Client,
    h1_client: &reqwest::Client,
    h2_client: &reqwest::Client,
    req: http::Request<hyper::body::Incoming>,
) -> impl Future<Output = Result<http::Response<reqwest::Body>, reqwest::Error>> + 'static + use<> {
    let (mut head, body) = req.into_parts();

    let Some(path_and_query) = head.uri.path_and_query() else {
        error!(uri = %head.uri, "Tunnel request was missing path");
        return std::future::ready(Ok(http::Response::builder()
            .status(http::StatusCode::BAD_REQUEST)
            .body(reqwest::Body::default())
            .unwrap()))
        .left_future();
    };

    let destination =
        match restate_cloud_tunnel_client::util::parse_tunnel_destination(path_and_query) {
            Ok(destination) => destination,
            Err(message) => {
                error!(uri = %head.uri, "Tunnel request had an invalid path ({})", message);
                return std::future::ready(Ok(http::Response::builder()
                    .status(http::StatusCode::BAD_REQUEST)
                    .body(reqwest::Body::default())
                    .unwrap()))
                .left_future();
            }
        };

    head.uri = destination;

    let client = match (
        head.uri.scheme_str(),
        head.headers
            .get(HTTP_VERSION)
            .map(http::HeaderValue::as_bytes),
    ) {
        // https urls will use an alpn client which supports http2 via alpn and http1.1, unless the user requested a particular version
        (Some("https"), None) => {
            // we don't want to force HTTP2 as we don't know if the destination accepts it
            // a request with HTTP_11 can still end up using h2 if the alpn agrees
            head.version = http::Version::HTTP_11;
            alpn_client
        }
        // cleartext requests where the user didn't request a specific version; use http2 prior-knowledge
        (_, None) => {
            head.version = http::Version::HTTP_2;
            h2_client
        }
        // https or cleartext requests where the user requested http2; use http2
        // prior-knowledge for cleartext, h2 in alpn otherwise
        (_, Some(b"HTTP/2.0")) => {
            head.version = http::Version::HTTP_2;
            h2_client
        }
        // https or cleartext requests where the user requested http1.1; use http1.1
        // will not use h2 even if the alpn supports it
        (_, Some(b"HTTP/1.1")) => {
            head.version = http::Version::HTTP_11;
            h1_client
        }
        (_, Some(other)) => {
            error!(uri = %head.uri, "Tunnel request had an invalid x-restate-tunnel-http-version ({})", String::from_utf8_lossy(other));
            return std::future::ready(Ok(http::Response::builder()
                .status(http::StatusCode::BAD_REQUEST)
                .body(reqwest::Body::default())
                .unwrap()))
            .left_future();
        }
    };

    let uri = head.uri.to_string();

    let span = info_span!("client_proxy", destination = %uri);

    // let hyper insert this using the real destination
    head.headers.remove(http::header::HOST);

    let fut = client
        .request(head.method, uri)
        .version(head.version)
        .body(reqwest::Body::wrap(body))
        .headers(head.headers)
        .send();

    async move {
        match fut.await {
            Ok(result) => {
                info!("Proxied request with status {}", result.status());

                Ok(result.into())
            }
            Err(err) => {
                error!("Failed to proxy request: {}", err);

                Ok(http::Response::builder()
                    .status(http::StatusCode::BAD_GATEWAY)
                    .body(reqwest::Body::default())
                    .unwrap())
            }
        }
    }
    .instrument(span)
    .right_future()
}
