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
//! Liveness detection is an HTTP/2 mechanism: the pings ride the h2 connection.
//! The Lambda API negotiates h2, but a connection that ends up on HTTP/1.1 - an
//! endpoint override to a gateway that only speaks h1 - keeps the old
//! kernel-bound behavior.
//!
//! The rest mirrors the relevant parts of `aws-smithy-http-client`: connection
//! capture and poisoning, error classification, proxy support, and TLS settings.
//! Re-check those pieces when upgrading the SDK.

use std::borrow::Cow;
use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
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
use http::uri::Scheme;
use hyper_rustls::{ConfigBuilderExt, HttpsConnector, MaybeHttpsStream};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::proxy::Tunnel;
use hyper_util::client::legacy::connect::{
    CaptureConnection, Connect, Connected, Connection, HttpConnector as TcpConnector, HttpInfo,
    capture_connection,
};
use hyper_util::client::proxy::matcher::Matcher;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::ServerName;
use rustls::{CipherSuite, ClientConfig};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tower::{Service, ServiceExt};

use restate_types::config::Http2KeepAliveOptions;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Matches the SDK's rustls cipher suites and trust roots. This deliberately does
/// not honor `SSLKEYLOGFILE`, because Lambda requests carry AWS credentials.
static AWS_TLS_CONFIG: LazyLock<ClientConfig> = LazyLock::new(|| {
    // The crypto provider is set explicitly because the ring and aws_lc_rs rustls
    // features are both active, which disables auto-installation.
    ClientConfig::builder_with_provider(Arc::new(restrict_ciphers(
        rustls::crypto::aws_lc_rs::default_provider(),
    )))
    .with_protocol_versions(rustls::DEFAULT_VERSIONS)
    .expect("default versions are supported")
    .with_native_roots()
    .expect("can load native certificates")
    .with_no_client_auth()
});

/// The handshake config for connections tunneled through a proxy. The direct path
/// gets ALPN from hyper-rustls, but the in-tunnel handshake is our own, and without
/// offering ALPN it would fall back to HTTP/1.1 and lose keep-alive liveness
/// detection. (The SDK's own tunnel offers no ALPN.)
static AWS_TUNNEL_TLS_CONFIG: LazyLock<Arc<ClientConfig>> = LazyLock::new(|| {
    let mut config = AWS_TLS_CONFIG.clone();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Arc::new(config)
});

/// The cipher suites the SDK offers to AWS endpoints, in the SDK's order.
fn restrict_ciphers(base: CryptoProvider) -> CryptoProvider {
    const SUITES: &[CipherSuite] = &[
        CipherSuite::TLS13_AES_256_GCM_SHA384,
        CipherSuite::TLS13_AES_128_GCM_SHA256,
        // TLS 1.2 suites
        CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
        CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
        CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
        CipherSuite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
    ];

    CryptoProvider {
        cipher_suites: SUITES
            .iter()
            .filter_map(|suite| {
                base.cipher_suites
                    .iter()
                    .find(|s| &s.suite() == suite)
                    .cloned()
            })
            .collect(),
        ..base
    }
}

pub(crate) fn from_options(keep_alive: &Http2KeepAliveOptions) -> SharedHttpClient {
    SharedHttpClient::new(AwsHttpClient {
        keep_alive: KeepAlive::from_options(keep_alive),
        // The same environment variables the SDK's default client reads
        // (`ProxyConfig::from_env()`, behavior versions since 2025-08-07).
        proxy: Arc::new(Matcher::from_env()),
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

/// Connector settings supplied by Smithy. Each distinct pair owns a pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Timeouts {
    connect: Option<Duration>,
    read: Option<Duration>,
}

#[derive(Debug)]
struct AwsHttpClient {
    keep_alive: KeepAlive,
    proxy: Arc<Matcher>,
    connectors: DashMap<Timeouts, SharedHttpConnector>,
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

        // This runs on every request, always with the same timeouts, so take the
        // shared-lock hit path before entry()'s exclusive shard lock.
        if let Some(connector) = self.connectors.get(&timeouts) {
            return connector.clone();
        }

        self.connectors
            .entry(timeouts)
            .or_insert_with(|| {
                SharedHttpConnector::new(AwsHttpConnector::new(
                    self.keep_alive,
                    timeouts,
                    Arc::clone(&self.proxy),
                ))
            })
            .clone()
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        Some(ConnectorMetadata::new("hyper", Some(Cow::Borrowed("1.x"))))
    }
}

#[derive(Debug, Clone)]
struct AwsHttpConnector<C = ConnectTimeout<ProxiedConnector>> {
    client: Client<C, SdkBody>,
    read_timeout: Option<Duration>,
    proxy: Arc<Matcher>,
}

impl AwsHttpConnector {
    fn new(keep_alive: KeepAlive, timeouts: Timeouts, proxy: Arc<Matcher>) -> Self {
        // The timeout covers proxy tunneling and the TLS handshake as well as the
        // TCP connect, so that a peer that goes silent midway through the handshake
        // is bounded too.
        let connector = ConnectTimeout {
            inner: ProxiedConnector::new(Arc::clone(&proxy)),
            timeout: timeouts.connect,
        };

        Self::with_connector(connector, keep_alive, timeouts.read, proxy)
    }
}

impl<C> AwsHttpConnector<C>
where
    C: Connect + Clone,
{
    fn with_connector(
        connector: C,
        keep_alive: KeepAlive,
        read_timeout: Option<Duration>,
        proxy: Arc<Matcher>,
    ) -> Self {
        let mut builder = Client::builder(TokioExecutor::default());
        builder
            .timer(TokioTimer::default())
            .pool_timer(TokioTimer::default())
            .http2_keep_alive_interval(keep_alive.ping_interval)
            .http2_keep_alive_timeout(keep_alive.ping_timeout);

        Self {
            client: builder.build(connector),
            read_timeout,
            proxy,
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

        // HTTPS proxy authentication belongs to CONNECT. Plain HTTP has no
        // tunnel, so match Smithy by adding it to the forwarded request.
        if request.uri().scheme() == Some(&Scheme::HTTP)
            && !request
                .headers()
                .contains_key(http::header::PROXY_AUTHORIZATION)
            && let Some(auth) = self
                .proxy
                .intercept(request.uri())
                .and_then(|intercept| intercept.basic_auth().cloned())
        {
            request
                .headers_mut()
                .insert(http::header::PROXY_AUTHORIZATION, auth);
        }

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

/// Establishes connections directly, or through the proxy configured via the
/// standard environment variables, reproducing the SDK default client's behavior:
/// plain-HTTP requests are forwarded through the proxy (in absolute form, via the
/// proxied-connection marker), and HTTPS is tunneled with `CONNECT` followed by a
/// TLS handshake inside the tunnel.
#[derive(Debug, Clone)]
struct ProxiedConnector {
    https: HttpsConnector<TcpConnector>,
    proxy: Arc<Matcher>,
}

impl ProxiedConnector {
    fn new(proxy: Arc<Matcher>) -> Self {
        let mut tcp_connector = TcpConnector::new();
        tcp_connector.enforce_http(false);
        tcp_connector.set_nodelay(true);

        // Not https_only: an endpoint override, such as a local test double, may be
        // plain HTTP.
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(AWS_TLS_CONFIG.clone())
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(tcp_connector);

        Self { https, proxy }
    }
}

type HttpsStream = MaybeHttpsStream<TokioIo<TcpStream>>;

impl Service<Uri> for ProxiedConnector {
    type Response = ProxiedConnection;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.https.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let Some(intercept) = self.proxy.intercept(&dst) else {
            let connect = self.https.call(dst);
            return Box::pin(async move {
                Ok(ProxiedConnection::Forward {
                    stream: Box::new(connect.await?),
                    via_proxy: false,
                })
            });
        };

        if dst.scheme() == Some(&Scheme::HTTPS) {
            let mut tunnel = Tunnel::new(intercept.uri().clone(), self.https.clone());
            if let Some(auth) = intercept.basic_auth() {
                tunnel = tunnel.with_auth(auth.clone());
            }

            Box::pin(async move {
                let host = dst.host().ok_or("missing host in URI for TLS handshake")?;
                let server_name = ServerName::try_from(host.to_owned())?;

                let tunneled = tunnel.oneshot(dst.clone()).await?;
                let stream = tokio_rustls::TlsConnector::from(AWS_TUNNEL_TLS_CONFIG.clone())
                    .connect(server_name, TokioIo::new(tunneled))
                    .await?;

                Ok(ProxiedConnection::Tunneled {
                    stream: Box::new(TokioIo::new(stream)),
                })
            })
        } else {
            let connect = self.https.call(intercept.uri().clone());
            Box::pin(async move {
                Ok(ProxiedConnection::Forward {
                    stream: Box::new(connect.await?),
                    via_proxy: true,
                })
            })
        }
    }
}

/// A connection from [`ProxiedConnector`], carrying the metadata hyper reads off
/// it: whether it goes through a proxy (plain-HTTP requests then use absolute
/// form) and whether ALPN selected h2.
// Streams are boxed because the TLS session state makes them large (over a
// kilobyte); connections are created rarely.
enum ProxiedConnection {
    Forward {
        stream: Box<HttpsStream>,
        via_proxy: bool,
    },
    Tunneled {
        stream: Box<TokioIo<TlsStream<TokioIo<HttpsStream>>>>,
    },
}

impl Connection for ProxiedConnection {
    fn connected(&self) -> Connected {
        match self {
            Self::Forward { stream, via_proxy } => stream.connected().proxy(*via_proxy),
            Self::Tunneled { stream } => {
                let (tunnel, session) = stream.inner().get_ref();
                let connected = tunnel.inner().connected().proxy(true);
                if session.alpn_protocol() == Some(b"h2") {
                    connected.negotiated_h2()
                } else {
                    connected
                }
            }
        }
    }
}

impl hyper::rt::Read for ProxiedConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Forward { stream, .. } => Pin::new(stream.as_mut()).poll_read(cx, buf),
            Self::Tunneled { stream } => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl hyper::rt::Write for ProxiedConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Forward { stream, .. } => Pin::new(stream.as_mut()).poll_write(cx, buf),
            Self::Tunneled { stream } => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Forward { stream, .. } => Pin::new(stream.as_mut()).poll_write_vectored(cx, bufs),
            Self::Tunneled { stream } => Pin::new(stream.as_mut()).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Forward { stream, .. } => stream.is_write_vectored(),
            Self::Tunneled { stream } => stream.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Forward { stream, .. } => Pin::new(stream.as_mut()).poll_flush(cx),
            Self::Tunneled { stream } => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Forward { stream, .. } => Pin::new(stream.as_mut()).poll_shutdown(cx),
            Self::Tunneled { stream } => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Ported from the SDK adapter's `extract_smithy_connection`.
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

/// Classifies transport errors the way the SDK's adapter does, so the Lambda
/// client's retry classifier sees the error kinds it expects. Ported from
/// `downcast_error`; one
/// divergence is that upstream first returns an already-built `ConnectorError`
/// found in the boxed error, which has no equivalent here because nothing in this
/// connector stack produces one.
fn connector_error(err: hyper_util::client::legacy::Error) -> ConnectorError {
    if find_source::<ConnectTimeoutError>(&err).is_some() {
        return ConnectorError::timeout(err.into());
    }

    if let Some(hyper_error) = find_source::<hyper::Error>(&err) {
        return classify_hyper_error(hyper_error)(err.into());
    }

    if err.is_connect() || find_source::<std::io::Error>(&err).is_some() {
        return ConnectorError::io(err.into());
    }

    ConnectorError::other(err.into(), None)
}

/// Ported from the SDK adapter's `to_connector_error`.
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
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder;
    use bytes::Bytes;
    use googletest::prelude::*;
    use http::{Response, StatusCode};
    // The proxy matcher's name collides with googletest's `Matcher` trait.
    use hyper_util::client::proxy::matcher::Matcher as ProxyMatcher;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use restate_util_time::{FriendlyDuration, NonZeroFriendlyDuration};

    use super::*;

    fn keep_alive(interval: Option<Duration>) -> KeepAlive {
        KeepAlive {
            ping_interval: interval,
            ping_timeout: Duration::from_millis(50),
        }
    }

    fn no_proxy() -> Arc<ProxyMatcher> {
        Arc::new(ProxyMatcher::builder().build())
    }

    async fn capturing_proxy(
        response: &'static [u8],
    ) -> (std::net::SocketAddr, Arc<Mutex<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let request = Arc::new(Mutex::new(String::new()));
        let captured = Arc::clone(&request);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let read = stream.read(&mut buf).await.unwrap();
            *captured.lock().unwrap() = String::from_utf8_lossy(&buf[..read]).into_owned();
            stream.write_all(response).await.unwrap();
        });
        (addr, request)
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
            no_proxy(),
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
            no_proxy(),
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

    #[tokio::test]
    async fn plain_http_proxy_uses_absolute_form_and_authentication() {
        let (proxy_addr, request_head) =
            capturing_proxy(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n").await;

        let proxy = ProxyMatcher::builder()
            .http(format!("http://Aladdin:opensesame@{proxy_addr}"))
            .build();
        let connector = AwsHttpConnector::new(
            keep_alive(None),
            Timeouts {
                connect: Some(Duration::from_secs(3)),
                read: None,
            },
            Arc::new(proxy),
        );

        let request = HttpRequest::try_from(
            http::Request::builder()
                .uri("http://lambda.example/invoke")
                .body(SdkBody::empty())
                .unwrap(),
        )
        .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(10), connector.call(request))
            .await
            .expect("the proxied request must not hang");

        assert_that!(
            response.map(|response| response.status().as_u16()),
            ok(eq(200))
        );
        // Absolute form proves the request was addressed to the proxy rather than
        // sent as if the proxy were the origin.
        assert_that!(
            *request_head.lock().unwrap(),
            all!(
                starts_with("GET http://lambda.example/invoke HTTP/1.1\r\n"),
                contains_substring("proxy-authorization: Basic QWxhZGRpbjpvcGVuc2VzYW1l\r\n")
            )
        );
    }

    #[tokio::test]
    async fn https_proxy_authenticates_connect() {
        let (proxy_addr, request_head) = capturing_proxy(
            b"HTTP/1.1 407 Proxy Authentication Required\r\ncontent-length: 0\r\n\r\n",
        )
        .await;

        let proxy = ProxyMatcher::builder()
            .https(format!("http://Aladdin:opensesame@{proxy_addr}"))
            .build();
        let connector = AwsHttpConnector::new(
            keep_alive(None),
            Timeouts {
                connect: Some(Duration::from_secs(3)),
                read: None,
            },
            Arc::new(proxy),
        );

        let result =
            tokio::time::timeout(Duration::from_secs(10), connector.call(invoke_request()))
                .await
                .expect("the proxy response must not hang");
        assert_that!(result, err(anything()));
        assert_that!(
            *request_head.lock().unwrap(),
            all!(
                starts_with("CONNECT lambda.example:443 HTTP/1.1\r\n"),
                contains_substring("Proxy-Authorization: Basic QWxhZGRpbjpvcGVuc2VzYW1l\r\n")
            )
        );
    }

    #[test]
    fn connectors_are_cached_per_timeout_setting() {
        let client = AwsHttpClient {
            keep_alive: KeepAlive::from_options(&Http2KeepAliveOptions::default()),
            proxy: no_proxy(),
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
