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
use std::fmt::Display;
use std::future::Future;

use std::pin::pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use anyhow::Result;
use http::uri::PathAndQuery;
use http::{Request, StatusCode, Uri};
use http_body_util::BodyExt;
use hyper::Response;
use hyper::body::Incoming;
use hyper_rustls::{ConfigBuilderExt, HttpsConnector};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioTimer;
use rustls::ClientConfig;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tower::Service;
use tracing::error;

pub static TLS_CLIENT_CONFIG: LazyLock<ClientConfig> = LazyLock::new(|| {
    ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .expect("default versions are supported")
        .with_native_roots()
        .expect("Can load native certificates")
        .with_no_client_auth()
});

pub struct Handler<Notify, Client> {
    https_connector: HttpsConnector<HttpConnector>,
    inner: Arc<HandlerInner<Notify, Client>>,
}

impl<N, C> Handler<N, C> {
    #[allow(clippy::too_many_arguments)]
    pub fn new<
        Notify: Fn(HandlerNotification),
        Client: hyper::service::Service<Request<Incoming>>,
    >(
        client: Client,
        connect_timeout: Duration,
        environment_id: &str,
        signing_public_key: &str,
        bearer_token: &str,
        force_tunnel_name: Option<String>,
        notify: Option<Notify>,
    ) -> Result<Handler<Notify, Client>, ServeError> {
        let request_identity_key = super::request_identity::parse_public_key(signing_public_key)
            .expect("Problem validating request identity public key");

        let mut http_connector = HttpConnector::new();
        http_connector.set_nodelay(true);
        http_connector.set_connect_timeout(Some(connect_timeout));
        http_connector.enforce_http(false);
        // default interval on linux is 75 secs, also use this as the start-after
        http_connector.set_keepalive(Some(Duration::from_secs(75)));

        let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(TLS_CLIENT_CONFIG.clone())
            .https_or_http()
            .enable_http2()
            .wrap_connector(http_connector);

        Ok(Handler {
            https_connector,
            inner: Arc::new(HandlerInner {
                tunnel_name: force_tunnel_name.map(OnceLock::from).unwrap_or_default(),
                request_identity_key,
                environment_id: environment_id.into(),
                bearer_token: bearer_token.into(),
                client,
                notify,
            }),
        })
    }
}

impl<N, C> Clone for Handler<N, C> {
    fn clone(&self) -> Self {
        Self {
            https_connector: self.https_connector.clone(),
            inner: self.inner.clone(),
        }
    }
}

pub struct HandlerInner<Notify, GetClient> {
    tunnel_name: OnceLock<String>,
    request_identity_key: jsonwebtoken::DecodingKey,
    environment_id: String,
    bearer_token: String,
    client: GetClient,
    notify: Option<Notify>,
}

struct ServeState {
    started: AtomicBool,
    start_result: AsyncOnce<Result<(), StartError>>,
    request_drain: CancellationToken,
    cancel: CancellationToken,
}

pub enum HandlerNotification {
    Started {
        proxy_port: u16,
        tunnel_url: String,
        tunnel_name: String,
    },
    Error(String),
    RequestIdentityError(String),
    Request,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum StartError {
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
    #[error("Timed out while waiting for tunnel handshake")]
    Timeout,
}

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("Failed to initialise tunnel: {0}")]
    StartError(#[from] StartError),
    #[error("Failed to serve over tunnel: {0}")]
    Hyper(#[source] hyper::Error),
    #[error("Failed to connect to tunnel server: {0}")]
    Connection(#[source] Box<dyn Error + Send + Sync>),
    #[error("Server closed connection while {0}")]
    ServerClosed(&'static str),
    #[error("Client closed connection while {0}")]
    ClientClosed(&'static str),
}

impl ServeError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::StartError(_) => false,
            Self::Hyper(_) => false,
            Self::Connection(_) => true,
            Self::ServerClosed(_) => true,
            Self::ClientClosed(_) => false,
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

impl<Notify, Client, ClientError, ClientFuture, ResponseBody> Handler<Notify, Client>
where
    Notify: Fn(HandlerNotification) + Send + Sync + 'static,
    Client: hyper::service::Service<
            Request<Incoming>,
            Error = ClientError,
            Future = ClientFuture,
            Response = Response<ResponseBody>,
        > + Send
        + Sync
        + 'static,
    ClientError: std::fmt::Display + Send + Sync + 'static,
    ClientFuture:
        Future<Output = Result<Response<ResponseBody>, ClientError>> + Send + Sync + 'static,
    ResponseBody: hyper::body::Body<Data = bytes::Bytes> + Send + Sync + 'static,
    ResponseBody::Error:
        Display + Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send + Sync + 'static,
{
    pub async fn serve(
        mut self,
        tunnel_url: Uri,
        cancel: CancellationToken,
        request_drain: CancellationToken,
    ) -> ServeError {
        let io = tokio::select! {
             io_result = self.https_connector.call(tunnel_url) => {
                 match io_result {
                     Ok(io) => io,
                     Err(err) => return ServeError::Connection(err),
                 }
             }
             _ = cancel.cancelled() => {
                 return ServeError::ClientClosed("connecting to tunnel")
             }
        };

        let inner = self.inner.clone();
        let serve_state = Arc::new(ServeState {
            started: AtomicBool::new(false),
            start_result: AsyncOnce::new(),
            cancel: cancel.clone(),
            request_drain,
        });

        {
            let server =
                hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    // use server-initiated http2 keepalives to detect stuck tunnel connections
                    .keep_alive_interval(Duration::from_secs(75))
                    .timer(TokioTimer::new())
                    .serve_connection(
                        io,
                        hyper::service::service_fn(|req| {
                            let inner = inner.clone();
                            let serve_state = serve_state.clone();
                            async move {
                                if !serve_state.started.swap(true, Ordering::Relaxed) {
                                    // we are the first request

                                    let body = req.into_body();

                                    {
                                        let inner = inner.clone();
                                        tokio::task::spawn(async move {
                                            match inner.process_start_trailers(body).await {
                                                Ok(()) => {
                                                    let _ = serve_state.start_result.set(Ok(()));
                                                }
                                                Err(err) => {
                                                    let _ = serve_state.start_result.set(Err(err));
                                                    serve_state.cancel.cancel();
                                                }
                                            }
                                        })
                                    };

                                    let resp = Response::builder()
                                        .header(
                                            "authorization",
                                            format!("Bearer {}", inner.bearer_token),
                                        )
                                        .header("environment-id", &inner.environment_id);

                                    let resp = if let Some(tunnel_name) = inner.tunnel_name.get() {
                                        resp.header("tunnel-name", tunnel_name)
                                    } else {
                                        resp
                                    };

                                    let resp = resp.header("supports-drain", "true");

                                    return Ok(resp
                                        .body(http_body_util::Either::Left(
                                            http_body_util::Empty::new(),
                                        ))
                                        .unwrap());
                                }

                                match serve_state.start_result.wait().await {
                                    Ok(()) => inner.proxy(req, &serve_state.request_drain).await,
                                    Err(err) => Err(err.clone()),
                                }
                            }
                        }),
                    );
            let mut server = pin!(server);

            let handshake_timeout = async {
                match tokio::time::timeout(Duration::from_secs(5), serve_state.start_result.wait())
                    .await
                {
                    Ok(Ok(_)) | Ok(Err(_)) => std::future::pending().await,
                    Err(_) => {}
                }
            };

            tokio::select! {
                server_result = &mut server => match server_result {
                    Ok(()) => {},
                    Err(err) => {
                        return err.into();
                    }
                },
                _ = handshake_timeout => {
                    let _ = serve_state.start_result.set(Err(StartError::Timeout));
                    serve_state.cancel.cancel();

                    if serve_state.started.load(Ordering::Relaxed) {
                        hyper::server::conn::http2::Connection::graceful_shutdown(server.as_mut());
                        // let the server drain, ignoring any errors
                        let _ = server.await;
                    }
                }
                _ = serve_state.cancel.cancelled() => {
                    if serve_state.started.load(Ordering::Relaxed) {
                        hyper::server::conn::http2::Connection::graceful_shutdown(server.as_mut());
                        // let the server drain, ignoring any errors
                        let _ = server.await;
                    }
                },
            }
        }

        match serve_state.start_result.get() {
            Some(Ok(())) if serve_state.cancel.is_cancelled() => {
                // if we are cancelled but not failed, someone higher up the call stack requested the cancellation
                ServeError::ClientClosed("proxying")
            }
            Some(Ok(())) => {
                // if we are not cancelled or failed, the tcp connection simply closed
                ServeError::ServerClosed("proxying")
            }
            // handshake failure
            Some(Err(err)) => err.clone().into(),
            // closed before we finished the handshake
            None => ServeError::ClientClosed("handshaking"),
        }
    }
}
impl<Notify, Client, ClientError, ClientFuture, ResponseBody> HandlerInner<Notify, Client>
where
    Notify: Fn(HandlerNotification) + Send + Sync + 'static,
    Client: hyper::service::Service<
            Request<Incoming>,
            Error = ClientError,
            Future = ClientFuture,
            Response = Response<ResponseBody>,
        > + Send
        + Sync
        + 'static,
    ClientError: std::fmt::Display + Send + Sync + 'static,
    ClientFuture:
        Future<Output = Result<Response<ResponseBody>, ClientError>> + Send + Sync + 'static,
    ResponseBody: hyper::body::Body<Data = bytes::Bytes> + Send + Sync + 'static,
    ResponseBody::Error:
        Display + Into<Box<dyn std::error::Error + Send + Sync + 'static>> + Send + Sync + 'static,
{
    async fn proxy(
        &self,
        req: Request<Incoming>,
        request_drain: &CancellationToken,
    ) -> Result<
        Response<http_body_util::Either<http_body_util::Empty<bytes::Bytes>, ResponseBody>>,
        StartError,
    > {
        // tunnel server used to provide invalid paths that didn't start with /
        let req = fix_path(req);

        // control message to allow health checks and load tests down the tunnel
        if req.uri().path() == "/_/health" {
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .body(http_body_util::Either::Left(http_body_util::Empty::new()))
                .unwrap());
        }

        // control message intended to advise us that the tunnel server is going away
        if req.uri().path() == "/_/drain-tunnel" {
            request_drain.cancel();
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .body(http_body_util::Either::Left(http_body_util::Empty::new()))
                .unwrap());
        }

        if let Err(err) = super::request_identity::validate_request_identity(
            &self.request_identity_key,
            req.headers(),
            req.uri().path(),
        ) {
            error!(%err, path = req.uri().path(), "Failed to validate request identity");

            if let Some(notify) = self.notify.as_ref() {
                (notify)(HandlerNotification::RequestIdentityError(err.to_string()));
            }

            return Ok(Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body(http_body_util::Either::Left(http_body_util::Empty::new()))
                .unwrap());
        }

        let response = match self.client.call(req).await {
            Ok(result) => result,
            Err(err) => {
                if let Some(notify) = self.notify.as_ref() {
                    (notify)(HandlerNotification::Error(err.to_string()));
                }

                return Ok(Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .body(http_body_util::Either::Left(http_body_util::Empty::new()))
                    .unwrap());
            }
        };
        if let Some(notify) = self.notify.as_ref() {
            (notify)(HandlerNotification::Request);
        }

        Ok(response.map(http_body_util::Either::Right))
    }

    async fn process_start_trailers(&self, body: Incoming) -> Result<(), StartError> {
        let collected = body.collect().await;
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

        let proxy_port = Uri::from_str(proxy_url)
            .expect("proxy url must be valid")
            .port()
            .expect("proxy url must have a port")
            .as_u16();

        let tunnel_name = match trailers.get("tunnel-name").and_then(|s| s.to_str().ok()) {
            Some(name) => name,
            None => {
                return Err(StartError::MissingTunnelName);
            }
        };

        // check that the server used the tunnel name we requested (if we requested one)
        if self.tunnel_name.get_or_init(|| tunnel_name.into()) != tunnel_name {
            return Err(StartError::TunnelNameMismatch);
        }

        if let Some(notify) = self.notify.as_ref() {
            (notify)(HandlerNotification::Started {
                proxy_port,
                tunnel_url: tunnel_url.into(),
                tunnel_name: tunnel_name.into(),
            });
        }

        Ok(())
    }
}

/// A OnceLock wrapper that allows async waiting for initialization
pub struct AsyncOnce<T> {
    cell: OnceLock<T>,
    notify: Notify,
}

impl<T> Default for AsyncOnce<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> AsyncOnce<T> {
    pub fn new() -> Self {
        Self {
            cell: OnceLock::new(),
            notify: Notify::new(),
        }
    }

    pub async fn wait(&self) -> &T {
        if let Some(value) = self.cell.get() {
            return value;
        }
        loop {
            let notified = self.notify.notified();
            // check again in case it was set between the get() and notified() call
            if let Some(value) = self.cell.get() {
                return value;
            }
            notified.await
        }
    }

    pub fn set(&self, value: T) -> Result<(), T> {
        match self.cell.set(value) {
            Ok(()) => {
                self.notify.notify_waiters();
                Ok(())
            }
            Err(value) => Err(value),
        }
    }

    pub fn get(&self) -> Option<&T> {
        self.cell.get()
    }
}

fn fix_path<B>(req: Request<B>) -> Request<B> {
    let (mut req_parts, body) = req.into_parts();

    let mut uri_parts = req_parts.uri.into_parts();

    uri_parts.path_and_query = match uri_parts.path_and_query {
        None => Some(PathAndQuery::from_static("/")),
        Some(path) if path.as_str().is_empty() => Some(PathAndQuery::from_static("/")),
        Some(path) if path.as_str().starts_with("/") => Some(path),
        Some(path) => Some(
            PathAndQuery::try_from(format!("/{}", path.as_str()))
                .expect("fix_path should not create an invalid path"),
        ),
    };

    req_parts.uri = Uri::from_parts(uri_parts).expect("fix_path should not create an invalid uri");

    Request::from_parts(req_parts, body)
}
