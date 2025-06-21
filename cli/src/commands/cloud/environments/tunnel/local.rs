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
use restate_cloud_tunnel_client::local::{HandlerNotification, ServeError};
use restate_types::retries::RetryPolicy;
use url::Url;

use crate::cli_env::CliEnv;
use crate::clients::cloud::generated::DescribeEnvironmentResponse;

use super::renderer::{LocalRenderer, TunnelRenderer};

pub(crate) async fn run_local(
    env: &CliEnv,
    client: reqwest::Client,
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

    let handler = restate_cloud_tunnel_client::local::Handler::<(), ()>::new(
        ReqwestHyperService {
            reqwest_to_hyper: move |req: http::Request<hyper::body::Incoming>| {
                reqwest_to_hyper(&client, req)
            },
        },
        CliContext::get().connect_timeout(),
        &environment_info.environment_id,
        &environment_info.signing_public_key,
        bearer_token,
        url,
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

                async move { handler.serve(tunnel_url).await }
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

struct ReqwestHyperService<ReqwestToHyper> {
    reqwest_to_hyper: ReqwestToHyper,
}

impl<
    ReqwestToHyper: Fn(http::Request<hyper::body::Incoming>) -> ClientFuture,
    ClientFuture: Future<Output = Result<http::Response<reqwest::Body>, reqwest::Error>>,
> hyper::service::Service<http::Request<hyper::body::Incoming>>
    for ReqwestHyperService<ReqwestToHyper>
{
    type Response = http::Response<reqwest::Body>;

    type Error = reqwest::Error;

    type Future = ClientFuture;

    fn call(&self, req: http::Request<hyper::body::Incoming>) -> Self::Future {
        (self.reqwest_to_hyper)(req)
    }
}

fn reqwest_to_hyper(
    client: &reqwest::Client,
    req: http::Request<hyper::body::Incoming>,
) -> impl Future<Output = Result<http::Response<reqwest::Body>, reqwest::Error>>
+ Send
+ Sync
// we require this fancy new syntax to promise rustc that the future does not capture the client
+ use<> {
    let (head, body) = req.into_parts();

    let url = head.uri.to_string();
    let request = client
        .request(head.method, url)
        .body(reqwest::Body::wrap(body))
        .headers(head.headers);

    async move {
        let mut result = request.send().await?;

        let mut response = http::Response::builder().status(result.status());
        if let Some(headers) = response.headers_mut() {
            std::mem::swap(headers, result.headers_mut())
        };

        Ok(response.body(reqwest::Body::from(result)).unwrap())
    }
}
