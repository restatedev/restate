// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use restate_cli_util::CliContext;
use restate_cloud_tunnel_client::client::{HandlerNotification, ServeError};
use restate_types::retries::RetryPolicy;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, info_span};
use url::Url;

use crate::cli_env::CliEnv;
use crate::clients::cloud::generated::DescribeEnvironmentResponse;

use super::renderer::{LocalRenderer, TunnelRenderer};

const HTTP_VERSION: http::HeaderName =
    http::HeaderName::from_static("x-restate-tunnel-http-version");
const HTTP_VERSION_11: http::HeaderValue = http::HeaderValue::from_static("HTTP/1.1");

pub(crate) async fn run_local(
    env: &CliEnv,
    h1_client: reqwest::Client,
    h2_client: reqwest::Client,
    bearer_token: &str,
    environment_info: DescribeEnvironmentResponse,
    opts: &super::Tunnel,
    tunnel_renderer: Arc<TunnelRenderer>,
) -> Result<(), ServeError> {
    let port = if let Some(port) = opts.local_port {
        port
    } else {
        return Ok(());
    };

    let retry_policy = RetryPolicy::exponential(
        Duration::from_millis(10),
        2.0,
        None,
        Some(Duration::from_secs(12)),
    );

    let url = Url::parse(&format!("http://localhost:{port}")).unwrap();

    let handler = restate_cloud_tunnel_client::client::Handler::<(), ()>::new(
        hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
            let client = if req.headers().get(HTTP_VERSION) == Some(&HTTP_VERSION_11) {
                &h1_client
            } else {
                &h2_client
            };
            do_proxy(client, &url, req)
        }),
        CliContext::get().connect_timeout(),
        &environment_info.environment_id,
        &environment_info.signing_public_key,
        bearer_token,
        opts.tunnel_name.clone(),
        {
            let tunnel_renderer = tunnel_renderer.clone();
            Some(move |notification| match notification {
                HandlerNotification::Started {
                    proxy_port,
                    tunnel_url,
                    tunnel_name,
                } => {
                    tunnel_renderer.local.get_or_init(|| {
                        LocalRenderer::new(
                            proxy_port,
                            tunnel_url,
                            tunnel_name,
                            environment_info.name.clone(),
                            port,
                        )
                    });

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

    retry_policy
        .retry_if(
            || {
                let tunnel_url = tunnel_renderer
                    .local
                    .get()
                    // use the url reported by the active tunnel session, if there has been one
                    .map(|l| l.tunnel_url.as_str())
                    // or use the one the user specifically requested
                    .or(opts.tunnel_url.as_deref())
                    // or just use the configured base url
                    .unwrap_or(env.config.cloud.tunnel_base_url.as_str())
                    .parse()
                    .unwrap();

                let handler = handler.clone();

                async move {
                    Err(handler
                        .serve(
                            tunnel_url,
                            CancellationToken::new(),
                            CancellationToken::new(),
                        )
                        .await)
                }
            },
            |err: &ServeError| {
                if !err.is_retryable() {
                    return false;
                }

                if tunnel_renderer.local.get().is_none() {
                    // no point retrying if we've never had a success; leave that up to the user
                    return false;
                };

                let err = match err.source() {
                    Some(source) => format!("{err}, retrying\n  Caused by: {source}"),
                    None => format!("{err}, retrying"),
                };
                tunnel_renderer.store_error(err);

                true
            },
        )
        .await
}

fn do_proxy(
    client: &reqwest::Client,
    destination: &Url,
    req: http::Request<hyper::body::Incoming>,
) -> impl Future<Output = Result<http::Response<reqwest::Body>, reqwest::Error>>
+ Send
+ Sync
// we require this fancy new syntax to promise rustc that the future does not capture the client
+ use<> {
    let url = if let Some(path) = req.uri().path_and_query() {
        destination.join(path.as_str()).unwrap()
    } else {
        destination.clone()
    };

    let span = info_span!("client_proxy", destination = %url);

    let (head, body) = req.into_parts();

    let request = client
        .request(head.method, url)
        .body(reqwest::Body::wrap(body))
        .headers(head.headers);

    async move {
        let mut result = match request.send().await {
            Ok(result) => {
                info!("Proxied request with status {}", result.status());
                result
            }
            Err(err) => {
                error!("Failed to proxy request: {}", err);
                return Err(err);
            }
        };

        let mut response = http::Response::builder().status(result.status());
        if let Some(headers) = response.headers_mut() {
            std::mem::swap(headers, result.headers_mut())
        };

        Ok(response.body(reqwest::Body::from(result)).unwrap())
    }
    .instrument(span)
}
