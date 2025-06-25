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
use std::fmt::{Display, Formatter};
use std::future::Future;

use std::pin::pin;
use std::str::FromStr;
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
use rustls::ClientConfig;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};
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
    status: HandlerStatus,
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
            status: HandlerStatus::AwaitingStart,
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

    fn tunnel_name(&self) -> Option<&str> {
        self.inner.tunnel_name.get().map(String::as_str)
    }
}

impl<N, C> Clone for Handler<N, C> {
    fn clone(&self) -> Self {
        Self {
            status: HandlerStatus::AwaitingStart,
            https_connector: self.https_connector.clone(),
            inner: self.inner.clone(),
        }
    }
}

pub struct HandlerInner<Notify, GetClient> {
    pub(super) tunnel_name: OnceLock<String>,
    request_identity_key: jsonwebtoken::DecodingKey,
    environment_id: String,
    bearer_token: String,
    client: GetClient,
    notify: Option<Notify>,
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
    ServerClosed(String),
    #[error("Client closed connection while {0}")]
    ClientClosed(String),
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

pub(crate) enum HandlerStatus {
    AwaitingStart,
    Proxying,
    Failed(StartError),
}

impl Display for HandlerStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerStatus::AwaitingStart => write!(f, "awaiting start"),
            HandlerStatus::Failed(_) => write!(f, "failed"),
            HandlerStatus::Proxying => write!(f, "proxying"),
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
    pub async fn serve(mut self, tunnel_url: Uri, token: CancellationToken) -> ServeError {
        let io = tokio::select! {
             io_result = self.https_connector.call(tunnel_url) => {
                 match io_result {
                     Ok(io) => io,
                     Err(err) => return ServeError::Connection(err),
                 }
             }
             _ = token.cancelled() => {
                 return ServeError::ClientClosed(self.status.to_string())
             }
        };

        let this = Arc::new(RwLock::new(self));

        {
            let server =
                hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(
                        io,
                        hyper::service::service_fn(|req| {
                            let this = this.clone();
                            let token = token.clone();
                            async move {
                                let guard = this.read().await;
                                match &guard.status {
                                    HandlerStatus::AwaitingStart => {
                                        drop(guard);
                                        let guard = this.write_owned().await;
                                        match &guard.status {
                                            // won the race; process start
                                            HandlerStatus::AwaitingStart => {
                                                Ok(Self::process_start(guard, req, token)?
                                                    .map(http_body_util::Either::Left))
                                            }
                                            // lost the race to someone that failed
                                            HandlerStatus::Failed(err) => Err(err.clone()),
                                            // lost the race but they succeeded; treat this as a normal proxy request
                                            HandlerStatus::Proxying => {
                                                let guard = guard.downgrade();
                                                Ok(guard.proxy(req).await?)
                                            }
                                        }
                                    }
                                    HandlerStatus::Proxying => Ok(guard.proxy(req).await?),
                                    HandlerStatus::Failed(err) => Err(err.clone()),
                                }
                            }
                        }),
                    );
            let mut server = pin!(server);

            tokio::select! {
                server_result = &mut server => match server_result {
                    Ok(()) => {},
                    Err(err) => {
                        return err.into();
                    }
                },
                _ = token.cancelled() => {
                    hyper::server::conn::http2::Connection::graceful_shutdown(server.as_mut());
                    // let the server drain, ignoring any errors
                    let _ = server.await;
                },
            }
        }

        let this = this.read().await;

        if let HandlerStatus::Failed(err) = &this.status {
            err.clone().into()
        } else if token.is_cancelled() {
            // if we are cancelled but not failed, someone higher up the call stack requested the cancellation
            ServeError::ClientClosed(this.status.to_string())
        } else {
            // if we are not cancelled or failed, the tcp connection simply closed
            ServeError::ServerClosed(this.status.to_string())
        }
    }

    fn process_start(
        mut this: OwnedRwLockWriteGuard<Self>,
        req: Request<Incoming>,
        token: CancellationToken,
    ) -> Result<Response<http_body_util::Empty<bytes::Bytes>>, StartError> {
        let body = req.into_body();

        let resp = Response::builder()
            .header(
                "authorization",
                format!("Bearer {}", this.inner.bearer_token),
            )
            .header("environment-id", &this.inner.environment_id);

        let resp = if let Some(tunnel_name) = this.tunnel_name() {
            resp.header("tunnel-name", tunnel_name)
        } else {
            resp
        };

        // keep holding the lock until this is complete; no other requests should be processed
        tokio::task::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(5), this.process_start_trailers(body))
                .await
            {
                Ok(Ok(())) => {
                    this.status = HandlerStatus::Proxying;
                }
                Ok(Err(err)) => {
                    this.status = HandlerStatus::Failed(err);
                    token.cancel();
                }
                Err(_timeout) => {
                    this.status = HandlerStatus::Failed(StartError::Timeout);
                    token.cancel();
                }
            }
        });

        Ok(resp.body(http_body_util::Empty::new()).unwrap())
    }

    fn proxy(
        &self,
        req: Request<Incoming>,
    ) -> impl Future<
        Output = Result<
            Response<http_body_util::Either<http_body_util::Empty<bytes::Bytes>, ResponseBody>>,
            StartError,
        >,
    > + Send
    + Sync
    + 'static {
        let inner = self.inner.clone();

        // tunnel server used to provide invalid paths that didn't start with /
        let req = fix_path(req);

        async move {
            if let Err(err) = super::request_identity::validate_request_identity(
                &inner.request_identity_key,
                req.headers(),
                req.uri().path(),
            ) {
                error!(%err, path = req.uri().path(), "Failed to validate request identity");

                if let Some(notify) = inner.notify.as_ref() {
                    (notify)(HandlerNotification::RequestIdentityError(err.to_string()));
                }

                return Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(http_body_util::Either::Left(http_body_util::Empty::new()))
                    .unwrap());
            }

            let response = match inner.client.call(req).await {
                Ok(result) => result,
                Err(err) => {
                    if let Some(notify) = inner.notify.as_ref() {
                        (notify)(HandlerNotification::Error(err.to_string()));
                    }

                    return Ok(Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .body(http_body_util::Either::Left(http_body_util::Empty::new()))
                        .unwrap());
                }
            };
            if let Some(notify) = inner.notify.as_ref() {
                (notify)(HandlerNotification::Request);
            }

            Ok(response.map(http_body_util::Either::Right))
        }
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
        if self.inner.tunnel_name.get_or_init(|| tunnel_name.into()) != tunnel_name {
            return Err(StartError::TunnelNameMismatch);
        }

        if let Some(notify) = self.inner.notify.as_ref() {
            (notify)(HandlerNotification::Started {
                proxy_port,
                tunnel_url: tunnel_url.into(),
                tunnel_name: tunnel_name.into(),
            });
        }

        Ok(())
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
