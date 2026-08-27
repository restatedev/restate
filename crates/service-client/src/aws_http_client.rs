// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! HTTP client for the AWS Lambda API.
//!
//! The SDK ships its own hyper-backed client, but exposes none of hyper's HTTP/2
//! settings. Each partition-local client multiplexes Lambda invocations onto
//! long-lived HTTP/2 connections with no liveness detection. If a connection is
//! silently blackholed, its in-flight requests hang until the kernel exhausts its
//! retransmission budget, roughly 15 minutes later. This client enables keep-alive
//! pings, which bound that delay to the ping interval plus its timeout.
//!
//! Everything else here reproduces the contract of the SDK's own adapter, which the
//! Lambda client's interceptors rely on: capturing the pooled connection so that it
//! can be poisoned after a transient failure, and classifying transport errors.

use std::borrow::Cow;
use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use aws_smithy_runtime_api::client::connection::{CaptureSmithyConnection, ConnectionMetadata};
use aws_smithy_runtime_api::client::connector_metadata::ConnectorMetadata;
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_smithy_types::retry::ErrorKind;
use dashmap::DashMap;
use h2::Reason;
use http::Uri;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::{
    CaptureConnection, Connect, HttpConnector as TcpConnector, HttpInfo, capture_connection,
};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tower::Service;

use restate_types::config::Http2KeepAliveOptions;

use crate::utils::TLS_CLIENT_CONFIG;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

pub(crate) fn from_options(keep_alive: &Http2KeepAliveOptions) -> SharedHttpClient {
    SharedHttpClient::new(AwsHttpClient {
        keep_alive: KeepAlive::from_options(keep_alive),
        connectors: Default::default(),
    })
}

#[derive(Debug, Clone, Copy)]
struct KeepAlive {
    /// `None` disables keep-alive pings.
    ping_interval: Option<Duration>,
    ping_timeout: Duration,
}

impl KeepAlive {
    fn from_options(options: &Http2KeepAliveOptions) -> Self {
        let ping_interval: Duration = options.http2_keep_alive_interval.into();

        Self {
            ping_interval: (!ping_interval.is_zero()).then_some(ping_interval),
            ping_timeout: options.http2_keep_alive_timeout.into(),
        }
    }
}

/// The timeouts the SDK asks for, which vary per operation and so cannot be baked
/// into a single connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Timeouts {
    connect: Option<Duration>,
    read: Option<Duration>,
}

#[derive(Debug, Clone)]
struct AwsHttpClient {
    keep_alive: KeepAlive,
    connectors: Arc<DashMap<Timeouts, SharedHttpConnector>>,
}

impl HttpClient for AwsHttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        let timeouts = Timeouts {
            connect: settings.connect_timeout(),
            read: settings.read_timeout(),
        };

        self.connectors
            .entry(timeouts)
            .or_insert_with(|| {
                SharedHttpConnector::new(AwsHttpConnector::new(self.keep_alive, timeouts))
            })
            .clone()
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        Some(ConnectorMetadata::new("hyper", Some(Cow::Borrowed("1.x"))))
    }
}

#[derive(Debug, Clone)]
struct AwsHttpConnector<C = ConnectTimeout<HttpsConnector<TcpConnector>>> {
    client: Client<C, SdkBody>,
    read_timeout: Option<Duration>,
}

impl AwsHttpConnector {
    fn new(keep_alive: KeepAlive, timeouts: Timeouts) -> Self {
        let mut tcp_connector = TcpConnector::new();
        tcp_connector.enforce_http(false);
        tcp_connector.set_nodelay(true);

        // Not https_only: an endpoint override, such as a local test double, may be
        // plain HTTP.
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(TLS_CLIENT_CONFIG.clone())
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(tcp_connector);
        // The timeout covers the TLS handshake as well as the TCP connect, so that a
        // peer that goes silent midway through the handshake is bounded too.
        let connector = ConnectTimeout {
            inner: connector,
            timeout: timeouts.connect,
        };

        Self::with_connector(connector, keep_alive, timeouts.read)
    }
}

impl<C> AwsHttpConnector<C>
where
    C: Connect + Clone,
{
    fn with_connector(connector: C, keep_alive: KeepAlive, read_timeout: Option<Duration>) -> Self {
        let mut builder = Client::builder(TokioExecutor::default());
        builder
            .timer(TokioTimer::default())
            .pool_timer(TokioTimer::default())
            .http2_keep_alive_interval(keep_alive.ping_interval)
            .http2_keep_alive_timeout(keep_alive.ping_timeout);

        Self {
            client: builder.build(connector),
            read_timeout,
        }
    }
}

impl<C> HttpConnector for AwsHttpConnector<C>
where
    C: Connect + Clone + Debug + Send + Sync + 'static,
{
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let client = self.client.clone();
        let read_timeout = self.read_timeout;

        let mut request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(err) => return HttpConnectorFuture::ready(Err(ConnectorError::user(err.into()))),
        };

        // Hands the pooled connection to the SDK's interceptors, which poison it
        // when a request against it fails transiently.
        let captured = capture_connection(&mut request);
        if let Some(capture) = request.extensions().get::<CaptureSmithyConnection>() {
            capture.set_connection_retriever(move || smithy_connection(&captured));
        }

        HttpConnectorFuture::new(async move {
            let response = match read_timeout {
                Some(read_timeout) => tokio::time::timeout(read_timeout, client.request(request))
                    .await
                    .map_err(|_| {
                        ConnectorError::timeout(
                            format!("no response received within {read_timeout:?}").into(),
                        )
                    })?,
                None => client.request(request).await,
            }
            .map_err(connector_error)?;

            let (parts, body) = response.into_parts();
            HttpResponse::try_from(http::Response::from_parts(
                parts,
                SdkBody::from_body_1_x(body),
            ))
            .map_err(|err| ConnectorError::other(err.into(), None))
        })
    }
}

fn smithy_connection(captured: &CaptureConnection) -> Option<ConnectionMetadata> {
    let poisoner = captured.clone();
    let metadata = captured.connection_metadata();
    let connection = metadata.as_ref()?;

    let mut extensions = http::Extensions::new();
    connection.get_extras(&mut extensions);
    let http_info = extensions.get::<HttpInfo>();

    let mut builder = ConnectionMetadata::builder()
        .proxied(connection.is_proxied())
        .poison_fn(move || match poisoner.connection_metadata().as_ref() {
            Some(connection) => connection.poison(),
            None => tracing::trace!("no connection existed to poison"),
        });
    builder
        .set_local_addr(http_info.map(|info| info.local_addr()))
        .set_remote_addr(http_info.map(|info| info.remote_addr()));

    Some(builder.build())
}

fn connector_error(err: hyper_util::client::legacy::Error) -> ConnectorError {
    let err: BoxError = err.into();

    if find_source::<ConnectTimeoutError>(err.as_ref()).is_some() {
        return ConnectorError::timeout(err);
    }

    if let Some(hyper_error) = find_source::<hyper::Error>(err.as_ref()) {
        return classify_hyper_error(hyper_error)(err);
    }

    if let Some(hyper_util_error) = find_source::<hyper_util::client::legacy::Error>(err.as_ref())
        && (hyper_util_error.is_connect()
            || find_source::<std::io::Error>(hyper_util_error).is_some())
    {
        return ConnectorError::io(err);
    }

    ConnectorError::other(err, None)
}

fn classify_hyper_error(err: &hyper::Error) -> fn(BoxError) -> ConnectorError {
    if err.is_timeout() {
        return ConnectorError::timeout;
    }
    if err.is_user() {
        return ConnectorError::user;
    }
    if err.is_closed() || err.is_canceled() || find_source::<std::io::Error>(err).is_some() {
        return ConnectorError::io;
    }
    if err.is_incomplete_message() {
        return |err| ConnectorError::other(err, Some(ErrorKind::TransientError));
    }
    if let Some(h2_error) = find_source::<h2::Error>(err)
        && (h2_error.is_go_away()
            || (h2_error.is_reset() && h2_error.reason() == Some(Reason::REFUSED_STREAM)))
    {
        return ConnectorError::io;
    }

    tracing::warn!(
        err = %DisplayErrorContext(err),
        "unrecognized error from Hyper; please report retryable errors"
    );
    |err| ConnectorError::other(err, None)
}

fn find_source<'a, E: Error + 'static>(err: &'a (dyn Error + 'static)) -> Option<&'a E> {
    let mut source = Some(err);
    while let Some(err) = source {
        if let Some(matched) = err.downcast_ref::<E>() {
            return Some(matched);
        }
        source = err.source();
    }
    None
}

/// Bounds how long establishing a connection may take, handshake included.
#[derive(Debug, Clone)]
struct ConnectTimeout<C> {
    inner: C,
    timeout: Option<Duration>,
}

#[derive(Debug, thiserror::Error)]
#[error("could not connect within {0:?}")]
struct ConnectTimeoutError(Duration);

impl<C> Service<Uri> for ConnectTimeout<C>
where
    C: Service<Uri>,
    C::Future: Send + 'static,
    C::Error: Into<BoxError>,
{
    type Response = C::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let connect = self.inner.call(uri);
        let timeout = self.timeout;

        Box::pin(async move {
            let Some(timeout) = timeout else {
                return connect.await.map_err(Into::into);
            };

            tokio::time::timeout(timeout, connect)
                .await
                .map_err(|_| ConnectTimeoutError(timeout))?
                .map_err(Into::into)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder;
    use bytes::Bytes;
    use googletest::prelude::*;
    use http::{Response, StatusCode};
    use hyper_util::client::legacy::connect::{Connected, Connection};
    use hyper_util::rt::TokioIo;
    use restate_util_time::{FriendlyDuration, NonZeroFriendlyDuration};

    fn keep_alive(interval: Option<Duration>) -> KeepAlive {
        KeepAlive {
            ping_interval: interval,
            ping_timeout: Duration::from_millis(50),
        }
    }

    fn invoke_request() -> HttpRequest {
        HttpRequest::try_from(
            http::Request::builder()
                .uri("https://lambda.example/invoke")
                .body(SdkBody::empty())
                .unwrap(),
        )
        .unwrap()
    }

    /// Serves h2 connections that go silent after accepting a request - answering
    /// neither the response nor the pings - until `recover` is set, after which
    /// requests are answered normally. This is the incident's connection as hyper
    /// saw it, followed by a healthy replacement.
    #[derive(Clone, Debug)]
    struct StallingPeer {
        connections: Arc<AtomicUsize>,
        recover: Arc<AtomicBool>,
        release_stalled_peer: Arc<tokio::sync::Notify>,
    }

    impl Service<Uri> for StallingPeer {
        type Response = DuplexConnection;
        type Error = BoxError;
        type Future =
            Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _uri: Uri) -> Self::Future {
            self.connections.fetch_add(1, Ordering::Relaxed);
            let recover = Arc::clone(&self.recover);
            let release_stalled_peer = Arc::clone(&self.release_stalled_peer);

            Box::pin(async move {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    let mut server = h2::server::Builder::new()
                        .handshake::<_, Bytes>(server)
                        .await
                        .unwrap();

                    while let Some(request) = server.accept().await {
                        let (_request, mut respond) = request.unwrap();
                        if !recover.load(Ordering::Relaxed) {
                            release_stalled_peer.notified().await;
                            return;
                        }
                        let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
                        respond.send_response(response, true).unwrap();
                    }
                });
                Ok(DuplexConnection(TokioIo::new(client)))
            })
        }
    }

    /// An in-memory stream that presents itself to hyper as a negotiated h2
    /// connection.
    #[derive(Debug)]
    struct DuplexConnection(TokioIo<tokio::io::DuplexStream>);

    impl Connection for DuplexConnection {
        fn connected(&self) -> Connected {
            Connected::new().negotiated_h2()
        }
    }

    impl hyper::rt::Read for DuplexConnection {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: hyper::rt::ReadBufCursor<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
        }
    }

    impl hyper::rt::Write for DuplexConnection {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().0).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
        }
    }

    /// The regression this client exists to prevent: a connection that goes silent
    /// mid-flight must fail its in-flight request on the keep-alive timeout instead
    /// of hanging until the kernel gives up on the socket, and the next request must
    /// then succeed on a fresh connection.
    #[tokio::test]
    async fn silent_connection_fails_fast_and_the_next_request_recovers() {
        let peer = StallingPeer {
            connections: Arc::new(AtomicUsize::new(0)),
            recover: Arc::new(AtomicBool::new(false)),
            release_stalled_peer: Arc::new(tokio::sync::Notify::new()),
        };
        let connector = AwsHttpConnector::with_connector(
            peer.clone(),
            keep_alive(Some(Duration::from_millis(50))),
            None,
        );

        let started = tokio::time::Instant::now();
        let stalled =
            tokio::time::timeout(Duration::from_secs(10), connector.call(invoke_request()))
                .await
                .expect("the keep-alive timeout must end the request");

        assert!(stalled.as_ref().is_err_and(|err| err.is_timeout()));
        assert_that!(started.elapsed(), lt(Duration::from_secs(5)));
        assert_that!(peer.connections.load(Ordering::Relaxed), eq(1));

        peer.recover.store(true, Ordering::Relaxed);
        peer.release_stalled_peer.notify_waiters();
        let recovered =
            tokio::time::timeout(Duration::from_secs(10), connector.call(invoke_request()))
                .await
                .expect("the retry must not hang");

        assert_that!(
            recovered.map(|response| response.status().as_u16()),
            ok(eq(200))
        );
        assert_that!(peer.connections.load(Ordering::Relaxed), eq(2));
    }

    #[tokio::test]
    async fn connect_timeout_covers_the_tls_handshake() {
        // Accepted by the kernel, but the handshake never completes.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let connector = AwsHttpConnector::new(
            keep_alive(None),
            Timeouts {
                connect: Some(Duration::from_millis(100)),
                read: None,
            },
        );

        let request = HttpRequest::try_from(
            http::Request::builder()
                .uri(format!("https://{addr}/"))
                .body(SdkBody::empty())
                .unwrap(),
        )
        .unwrap();

        let started = tokio::time::Instant::now();
        let result = connector.call(request).await;
        assert!(result.as_ref().is_err_and(|err| err.is_timeout()));
        // The connect itself succeeds, so reaching the timeout is what ends the
        // attempt - and it must end there rather than hanging on the handshake.
        assert_that!(
            started.elapsed(),
            all!(ge(Duration::from_millis(100)), lt(Duration::from_secs(5)))
        );
    }

    #[test]
    fn connectors_are_cached_per_timeout_setting() {
        let client = AwsHttpClient {
            keep_alive: KeepAlive::from_options(&Http2KeepAliveOptions::default()),
            connectors: Default::default(),
        };
        let components = RuntimeComponentsBuilder::for_tests().build().unwrap();

        let settings = HttpConnectorSettings::builder()
            .connect_timeout(Duration::from_secs(3))
            .build();
        client.http_connector(&settings, &components);
        client.http_connector(&settings, &components);
        assert_that!(client.connectors.len(), eq(1));

        let other = HttpConnectorSettings::builder()
            .connect_timeout(Duration::from_secs(5))
            .build();
        client.http_connector(&other, &components);
        assert_that!(client.connectors.len(), eq(2));
    }

    #[test]
    fn a_zero_interval_disables_pings() {
        let disabled = KeepAlive::from_options(&Http2KeepAliveOptions {
            http2_keep_alive_interval: FriendlyDuration::ZERO,
            http2_keep_alive_timeout: NonZeroFriendlyDuration::from_secs_unchecked(20),
            http2_keep_alive_jitter: 0.2,
        });
        assert_that!(disabled.ping_interval, none());

        let default = KeepAlive::from_options(&Http2KeepAliveOptions::default());
        assert_that!(default.ping_interval, some(eq(Duration::from_secs(40))));
        assert_that!(default.ping_timeout, eq(Duration::from_secs(20)));
    }
}
