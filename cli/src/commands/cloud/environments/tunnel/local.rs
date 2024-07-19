// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result;
use futures::FutureExt;
use http::{Request, StatusCode, Uri};
use http_body::Body;
use hyper::client::HttpConnector;
use hyper::service::Service;
use hyper::Response;
use hyper_rustls::HttpsConnector;
use restate_cli_util::CliContext;
use restate_types::retries::RetryPolicy;
use tracing::{error, info};
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

    let mut http_connector = HttpConnector::new();
    http_connector.set_nodelay(true);
    http_connector.set_connect_timeout(Some(CliContext::get().connect_timeout()));
    http_connector.enforce_http(false);
    // default interval on linux is 75 secs, also use this as the start-after
    http_connector.set_keepalive(Some(Duration::from_secs(75)));

    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http2()
        .wrap_connector(http_connector);

    let request_identity_key =
        environment_info
            .signing_public_key
            .as_deref()
            .map(|signing_public_key| {
                super::request_identity::parse_public_key(signing_public_key)
                    .expect("Problem validating request identity public key")
            });

    let retry_policy = RetryPolicy::exponential(
        Duration::from_millis(10),
        2.0,
        None,
        Some(Duration::from_secs(12)),
    );

    // use a closure as impl trait associated types are not yet allowed
    let proxy = |inner, request| async move { proxy(inner, request).await };

    let url = Url::parse(&format!("http://localhost:{}", port)).unwrap();

    let handler = Handler {
        status: HandlerStatus::AwaitingStart,
        https_connector,
        inner: Arc::new(HandlerInner {
            tunnel_renderer,
            request_identity_key,
            environment_id: environment_info.environment_id,
            environment_name: environment_info.name,
            bearer_token: bearer_token.into(),
            client,
            url,
            opts: opts.clone(),
        }),
        proxy,
    };

    retry_policy
        .retry_if(
            || {
                let tunnel_url = handler
                    .tunnel_uri()
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

                if handler.inner.tunnel_renderer.local.get().is_none() {
                    // no point retrying if we've never had a success; leave that up to the user
                    return false;
                };

                let err = match err.source() {
                    Some(source) => format!("{err}, retrying\n  Caused by: {source}"),
                    None => format!("{err}, retrying"),
                };
                handler.inner.tunnel_renderer.store_error(err);

                true
            },
        )
        .await
}

pub(crate) struct Handler<Proxy> {
    pub status: HandlerStatus,
    pub https_connector: HttpsConnector<HttpConnector>,
    pub inner: Arc<HandlerInner>,
    pub proxy: Proxy,
}

impl<Proxy> Handler<Proxy> {
    fn tunnel_name(&self) -> Option<String> {
        if let Some(local_renderer) = self.inner.tunnel_renderer.local.get() {
            // already had a tunnel at a particular name, continue to use it
            Some(local_renderer.tunnel_name.clone())
        } else {
            // no tunnel yet; return the user-provided tunnel name if any
            self.inner.opts.tunnel_name.clone()
        }
    }

    pub fn tunnel_uri(&self) -> Option<&str> {
        if let Some(tunnel_renderer) = self.inner.tunnel_renderer.local.get() {
            // already had a tunnel at a particular url, continue to use it
            Some(&tunnel_renderer.tunnel_url)
        } else {
            // no tunnel yet; return the user-provided tunnel url if any
            self.inner.opts.tunnel_url.as_deref()
        }
    }
}

impl<Proxy: Clone> Clone for Handler<Proxy> {
    fn clone(&self) -> Self {
        Self {
            status: HandlerStatus::AwaitingStart,
            https_connector: self.https_connector.clone(),
            inner: self.inner.clone(),
            proxy: self.proxy.clone(),
        }
    }
}

pub(crate) struct HandlerInner {
    pub opts: super::Tunnel,
    pub tunnel_renderer: Arc<TunnelRenderer>,
    pub request_identity_key: Option<jsonwebtoken::DecodingKey>,
    pub environment_id: String,
    pub bearer_token: String,
    pub client: reqwest::Client,
    pub environment_name: String,
    pub url: Url,
}

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum StartError {
    #[error("Missing trailers")]
    MissingTrailers,
    #[error("Problem reading http2 stream")]
    Read,
    #[error("Bad status: {0}")]
    BadStatus(String),
    #[error("Missing status")]
    MissingStatus,
    #[error("Missing proxy URL")]
    MissingProxyURL,
    #[error("Missing tunnel URL")]
    MissingTunnelURL,
    #[error("Missing tunnel name")]
    MissingTunnelName,
    #[error("Tunnel service provided a different tunnel name to what we requested")]
    TunnelNameMismatch,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ServeError {
    #[error("Failed to initialise tunnel")]
    StartError(#[from] StartError),
    #[error("Failed to serve over tunnel")]
    Hyper(#[source] hyper::Error),
    #[error("Failed to connect to tunnel server")]
    Connection(#[source] Box<dyn Error + Send + Sync>),
    #[error("Server closed connection while {0}")]
    ServerClosed(String),
}

impl ServeError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::StartError(_) => false,
            Self::Hyper(_) => false,
            Self::Connection(_) => true,
            Self::ServerClosed(_) => true,
        }
    }
}

impl From<hyper::Error> for ServeError {
    fn from(err: hyper::Error) -> Self {
        if let Some(err) = err
            .source()
            .and_then(|err| err.downcast_ref::<StartError>())
        {
            // this can happen when ProcessingStart future returns an error to poll_ready
            Self::StartError(err.clone())
        } else {
            // generic hyper serving error; not sure how to hit this
            Self::Hyper(err)
        }
    }
}

pub(crate) enum HandlerStatus {
    AwaitingStart,
    ProcessingStart(Pin<Box<dyn Future<Output = Result<(), StartError>> + Send + Sync>>),
    Proxying,
}

impl Display for HandlerStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerStatus::AwaitingStart => write!(f, "awaiting start"),
            HandlerStatus::ProcessingStart(_) => write!(f, "processing start"),
            HandlerStatus::Proxying => write!(f, "proxying"),
        }
    }
}

impl<Proxy, ProxyFut> Handler<Proxy>
where
    Proxy: FnMut(Arc<HandlerInner>, reqwest::Request) -> ProxyFut,
    ProxyFut: Future<Output = Result<Response<hyper::Body>, StartError>> + Send + 'static,
{
    pub async fn serve(mut self, tunnel_url: Uri) -> Result<(), ServeError> {
        let io = self
            .https_connector
            .call(tunnel_url)
            .await
            .map_err(ServeError::Connection)?;

        #[allow(deprecated)]
        hyper::server::conn::Http::new()
            .serve_connection(io, &mut self)
            .await?;

        // there is a race where the server closes the connection before we process the trailers, leading
        // us to not observe a permanent StartError error like unauthorized. so, we should here process the future
        // to completion
        if let HandlerStatus::ProcessingStart(fut) = &mut self.status {
            fut.await?;
        }

        Err(ServeError::ServerClosed(self.status.to_string()))
    }
}

impl<Proxy, ProxyFut> Service<Request<hyper::Body>> for Handler<Proxy>
where
    Proxy: FnMut(Arc<HandlerInner>, reqwest::Request) -> ProxyFut,
    ProxyFut: Future<Output = Result<Response<hyper::Body>, StartError>>,
{
    type Response = Response<hyper::Body>;
    type Error = StartError;
    type Future = futures::future::Either<
        futures::future::Ready<Result<Self::Response, Self::Error>>,
        ProxyFut,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.status {
            HandlerStatus::AwaitingStart | HandlerStatus::Proxying { .. } => Poll::Ready(Ok(())),
            HandlerStatus::ProcessingStart(fut) => match fut.poll_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    self.status = HandlerStatus::Proxying;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn call(&mut self, req: Request<hyper::Body>) -> Self::Future {
        match &self.status {
            HandlerStatus::AwaitingStart => {
                let body: hyper::Body = req.into_body();

                self.status = HandlerStatus::ProcessingStart(Box::pin(process_start(
                    self.inner.clone(),
                    body,
                )));

                let resp = Response::builder()
                    .header(
                        "authorization",
                        format!("Bearer {}", self.inner.bearer_token),
                    )
                    .header("environment-id", &self.inner.environment_id);

                let resp = if let Some(tunnel_name) = self.tunnel_name() {
                    resp.header("tunnel-name", tunnel_name)
                } else {
                    resp
                };

                futures::future::ready(Ok(resp.body(hyper::Body::empty()).unwrap())).left_future()
            }
            HandlerStatus::ProcessingStart(_) => {
                // 'Implementations are permitted to panic if call is invoked without obtaining Poll::Ready(Ok(())) from poll_ready.'
                panic!("Called when not ready")
            }
            HandlerStatus::Proxying => {
                let url = if let Some(path) = req.uri().path_and_query() {
                    self.inner.url.join(path.as_str()).unwrap()
                } else {
                    self.inner.url.clone()
                };

                info!("Proxying request to {}", url);

                let (head, body) = req.into_parts();

                let request = self
                    .inner
                    .client
                    .request(head.method, url)
                    .body(body)
                    .headers(head.headers)
                    .build()
                    .expect("Failed to build request");

                (self.proxy)(self.inner.clone(), request).right_future()
            }
        }
    }
}

async fn process_start(inner: Arc<HandlerInner>, body: hyper::Body) -> Result<(), StartError> {
    let collected = Body::collect(body).await;
    let trailers = match collected {
        Ok(ref collected) if collected.trailers().is_some() => collected.trailers().unwrap(),
        Ok(_) => {
            return Err(StartError::MissingTrailers);
        }
        Err(_) => {
            return Err(StartError::Read);
        }
    };

    match trailers.get("tunnel-status").and_then(|s| s.to_str().ok()) {
        Some("ok") => {}
        Some(other) => {
            return Err(StartError::BadStatus(other.into()));
        }
        None => {
            return Err(StartError::MissingStatus);
        }
    }

    let proxy_url = match trailers.get("proxy-url").and_then(|s| s.to_str().ok()) {
        Some(url) => url,
        None => {
            return Err(StartError::MissingProxyURL);
        }
    };

    let tunnel_url = match trailers.get("tunnel-url").and_then(|s| s.to_str().ok()) {
        Some(url) => url,
        None => {
            return Err(StartError::MissingTunnelURL);
        }
    };

    let proxy_port = Url::parse(proxy_url)
        .expect("proxy url must be valid")
        .port()
        .expect("proxy url must have a port");

    let tunnel_name = match trailers.get("tunnel-name").and_then(|s| s.to_str().ok()) {
        Some(name) => name,
        None => {
            return Err(StartError::MissingTunnelName);
        }
    };

    if let Some(requested_tunnel_name) = inner.opts.tunnel_name.as_deref() {
        // the user provided a particular tunnel name; check that the server respected this
        if tunnel_name != requested_tunnel_name {
            return Err(StartError::TunnelNameMismatch);
        }
    }

    inner.tunnel_renderer.local.get_or_init(|| {
        LocalRenderer::new(
            proxy_port,
            tunnel_url.into(),
            tunnel_name.into(),
            inner.environment_name.clone(),
            inner.opts.local_port.unwrap(),
        )
    });

    inner.tunnel_renderer.clear_error();

    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum ProxyError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error("Failed to validate request identity.\nCould a different environment be trying to use your tunnel?\n{0}")]
    RequestIdentity(#[from] super::request_identity::Error),
}

pub(crate) async fn proxy(
    inner: Arc<HandlerInner>,
    request: reqwest::Request,
) -> Result<Response<hyper::Body>, StartError> {
    if let Some(request_identity_key) = &inner.request_identity_key {
        if let Err(err) = super::request_identity::validate_request_identity(
            request_identity_key,
            request.headers(),
            request.url().path(),
        ) {
            error!("Failed to validate request identity: {}", err);

            inner.tunnel_renderer.store_error(format!(
                "Request identity error, are you discovering from the right environment?\n  {err}"
            ));

            return Ok(Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body(hyper::Body::empty())
                .unwrap());
        }
    }

    let mut result = match inner.client.execute(request).await {
        Ok(result) => result,
        Err(err) => {
            error!("Failed to proxy request: {}", err);

            inner.tunnel_renderer.store_error(err);

            return Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(hyper::Body::empty())
                .unwrap());
        }
    };
    info!("Request proxied with status {}", result.status());
    inner.tunnel_renderer.clear_error();

    let mut response = Response::builder().status(result.status());
    if let Some(headers) = response.headers_mut() {
        std::mem::swap(headers, result.headers_mut())
    };

    Ok(response
        .body(hyper::Body::wrap_stream(result.bytes_stream()))
        .unwrap())
}
