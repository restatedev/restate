// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use http::Uri;
use hyper::body::{Body, Incoming};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use rustls::pki_types::ServerName;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio_util::either::Either;
use tonic::transport::{Channel, Endpoint};
use tower::Service;
use tracing::{Instrument, Span, debug, error_span, info, instrument, trace};

use restate_types::config::{Configuration, TlsMode};
use restate_types::errors::GenericError;
use restate_types::net::address::{AdvertisedAddress, GrpcPort};
use restate_types::net::address::{ListenerPort, PeerNetAddress};
use restate_types::net::connect_opts::CommonClientConnectionOptions;
use restate_types::net::listener::Listeners;

use crate::network::tls::{TlsClientConfig, TlsServerConfig};
use crate::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};

pub enum DNSResolution {
    // use whatever order getaddressinfo returns (http connector will use the first v4 and v6 ips it finds)
    Gai,
    // pick a single random v4 and v6 ip; useful where the record points to multiple distinct nodes
    Headless,
}

/// Derives the rustls server name for a fabric peer URI.
fn tls_server_name(uri: &Uri) -> Result<ServerName<'static>, String> {
    let host = uri
        .host()
        .ok_or_else(|| format!("fabric peer address '{uri}' has no host"))?;
    // URI hosts retain brackets around IPv6 addresses, while rustls
    // ServerName expects the unbracketed address.
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    ServerName::try_from(host.to_owned())
        .map_err(|_| format!("fabric peer host '{host}' is not a valid TLS server name"))
}

#[derive(Clone)]
struct ReloadingTlsConnector<C> {
    inner: C,
    tls_config: TlsClientConfig,
    server_name: Result<ServerName<'static>, String>,
    connect_timeout: Option<Duration>,
}

impl<C> ReloadingTlsConnector<C> {
    fn new(
        inner: C,
        uri: &Uri,
        tls_config: TlsClientConfig,
        connect_timeout: Option<Duration>,
    ) -> Self {
        Self {
            inner,
            tls_config,
            server_name: tls_server_name(uri),
            connect_timeout,
        }
    }
}

impl<C, IO> Service<Uri> for ReloadingTlsConnector<C>
where
    C: Service<Uri, Response = TokioIo<IO>> + Send + 'static,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<GenericError>,
{
    type Response = TokioIo<tokio_rustls::client::TlsStream<IO>>;
    type Error = GenericError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let connect = self.inner.call(uri);
        let tls_config = self.tls_config.clone();
        let server_name = self.server_name.clone();
        let connect_timeout = self.connect_timeout;

        Box::pin(async move {
            let connect = async move {
                let server_name = server_name.map_err(GenericError::from)?;
                let io = connect.await.map_err(Into::into)?;
                // Resolve the ArcSwap only after TCP connects, immediately
                // before the TLS handshake, so reconnects use the latest
                // materials.
                let io = tls_config
                    .tls_connector()
                    .connect(server_name, io.into_inner())
                    .await?;
                if io.get_ref().1.alpn_protocol() != Some(b"h2") {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "HTTP/2 was not negotiated",
                    )
                    .into());
                }
                Ok(TokioIo::new(io))
            };

            match connect_timeout {
                Some(timeout) => tokio::time::timeout(timeout, connect)
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection timed out"))?,
                None => connect.await,
            }
        })
    }
}

fn apply_http_options<R>(endpoint: &Endpoint, http: &mut HttpConnector<R>) {
    http.enforce_http(false);
    http.set_nodelay(endpoint.get_tcp_nodelay());
    http.set_keepalive(endpoint.get_tcp_keepalive());
    http.set_keepalive_interval(endpoint.get_tcp_keepalive_interval());
    http.set_keepalive_retries(endpoint.get_tcp_keepalive_retries());
    http.set_connect_timeout(endpoint.get_connect_timeout());
}

pub(crate) fn connect_tonic_endpoint(
    endpoint: Endpoint,
    uri: &Uri,
    dns_resolution: DNSResolution,
    tls_config: Option<&TlsClientConfig>,
) -> Channel {
    match (dns_resolution, tls_config) {
        (DNSResolution::Gai, None) => endpoint.connect_lazy(),
        (DNSResolution::Gai, Some(tls_config)) => {
            let mut http = HttpConnector::new();
            apply_http_options(&endpoint, &mut http);
            Channel::new(
                ReloadingTlsConnector::new(
                    http,
                    uri,
                    tls_config.clone(),
                    endpoint.get_connect_timeout(),
                ),
                endpoint,
            )
        }
        (DNSResolution::Headless, tls_config) => {
            // Headless DNS names need special consideration:
            // 1. We need to ensure all IPs are used across retries.
            // 2. The HTTP connector splits the connection timeout between all
            //    resolved addresses, so we don't want too many.
            let mut http = HttpConnector::new_with_resolver(RandomAddressResolver);
            apply_http_options(&endpoint, &mut http);

            if let Some(tls_config) = tls_config {
                Channel::new(
                    ReloadingTlsConnector::new(
                        http,
                        uri,
                        tls_config.clone(),
                        endpoint.get_connect_timeout(),
                    ),
                    endpoint,
                )
            } else {
                endpoint.connect_with_connector_lazy(http)
            }
        }
    }
}

pub fn create_tonic_channel<
    T: CommonClientConnectionOptions + Send + Sync + ?Sized,
    P: ListenerPort + GrpcPort,
>(
    address: AdvertisedAddress<P>,
    options: &T,
    dns_resolution: DNSResolution,
) -> Channel {
    let address = address.into_address().expect("valid address");
    let endpoint = match &address {
        PeerNetAddress::Uds(_) => {
            // dummy endpoint required to specify an uds connector, it is not used anywhere
            Endpoint::try_from("http://127.0.0.1").expect("/ should be a valid Uri")
        }
        PeerNetAddress::Http(uri) => Channel::builder(uri.clone()),
    };

    let endpoint = apply_options(endpoint, options);

    // Fabric peers that advertise https:// require the fabric client TLS
    // identity regardless of which channel factory dials them (metadata-store,
    // raft, control channels all go through here).
    let tls_config = address.is_tls().then(TlsClientConfig::global).flatten();

    match address {
        PeerNetAddress::Uds(uds_path) => {
            endpoint.connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
                let uds_path = uds_path.clone();
                async move {
                    Ok::<_, io::Error>(TokioIo::new(UnixStream::connect(uds_path).await?))
                }
            }))
        }
        PeerNetAddress::Http(uri) => {
            connect_tonic_endpoint(endpoint, &uri, dns_resolution, tls_config)
        }
    }
}

fn apply_options<T: CommonClientConnectionOptions + Send + Sync + ?Sized>(
    endpoint: Endpoint,
    options: &T,
) -> Endpoint {
    if let Some(request_timeout) = options.request_timeout() {
        endpoint.timeout(request_timeout)
    } else {
        endpoint
    }
    .connect_timeout(options.connect_timeout())
    .http2_keep_alive_interval(options.keep_alive_interval())
    .keep_alive_timeout(options.keep_alive_timeout())
    .http2_adaptive_window(options.http2_adaptive_window())
    // this true by default, but this is to guard against any change in defaults
    .tcp_nodelay(true)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("failed handling hyper connection: {0}")]
    HandlingConnection(#[from] GenericError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("configuration error: {0}")]
    Configuration(String),
}

#[instrument(
    level = "error",
    name = "server",
    skip_all,
    fields(server_name = %P::NAME, uds.path = tracing::field::Empty, server.address = tracing::field::Empty, server.port = tracing::field::Empty)
)]
pub async fn run_hyper_server<P: ListenerPort, S, B>(
    listeners: Listeners<P>,
    service: S,
    on_stop: impl Fn(),
    tls: Option<TlsServerConfig>,
) -> Result<(), Error>
where
    S: hyper::service::Service<http::Request<Incoming>, Response = hyper::Response<B>>
        + Send
        + Clone
        + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    if let Some(uds_path) = listeners.uds_address() {
        Span::current().record("uds.path", uds_path.display().to_string());
    }

    if let Some(socket_addr) = listeners.tcp_address() {
        Span::current().record("server.address", socket_addr.ip().to_string());
        Span::current().record("server.port", socket_addr.port());
    }

    if tls.is_some() {
        info!("Server listening with TLS enabled");
    } else {
        info!("Server listening");
    }
    run_listener_loop(listeners, service, P::NAME, tls).await?;
    on_stop();

    info!("Stopped listening");

    Ok(())
}

async fn run_listener_loop<P: ListenerPort, S, B>(
    mut listeners: Listeners<P>,
    service: S,
    server_name: &'static str,
    tls: Option<TlsServerConfig>,
) -> Result<(), Error>
where
    S: hyper::service::Service<http::Request<Incoming>, Response = hyper::Response<B>>
        + Send
        + Clone
        + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let mut configuration = Configuration::live();
    let mut shutdown = std::pin::pin!(cancellation_watcher());
    let graceful_shutdown = GracefulShutdown::new();
    let task_name: Arc<str> = Arc::from(format!("{server_name}-socket"));

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                debug!("Shutdown requested, will stop listening to new connections");
                drop(listeners);
                break;
            }
            incoming_connection = listeners.accept() => {
                let (stream, peer_addr) = incoming_connection?;
                let socket_span = error_span!("SocketHandler", ?peer_addr);

                let config = configuration.live_load();
                let network_options = &config.networking;
                let mut builder = hyper_util::server::conn::auto::Builder::new(TaskCenterExecutor);
                builder
                    .http2()
                    .timer(hyper_util::rt::TokioTimer::default())
                    .adaptive_window(network_options.http2_adaptive_window)
                    .initial_connection_window_size(network_options.connection_window_size())
                    .initial_stream_window_size(network_options.stream_window_size())
                    .keep_alive_interval(Some(network_options.http2_keep_alive_interval.into()))
                    .keep_alive_timeout(network_options.http2_keep_alive_timeout.into());

                match stream {
                    Either::Left(tcp_stream) => {
                        let tls = tls.clone();
                        let service = service.clone();
                        let graceful_shutdown = graceful_shutdown.watcher();
                        let tls_mode = if P::SUPPORTS_TLS {
                            config.common.fabric_tls_mode()
                        } else {
                            TlsMode::Off
                        };

                        TaskCenter::spawn(TaskKind::SocketHandler, task_name.clone(), async move {
                            let negotiated = tokio::time::timeout(
                                TLS_NEGOTIATION_TIMEOUT,
                                negotiate_tcp_transport(tcp_stream, tls, tls_mode),
                            )
                            .await;
                            let stream = match negotiated {
                                Ok(Ok(stream)) => stream,
                                Ok(Err(e)) => {
                                    debug!("TCP transport negotiation failed: {e}");
                                    return Ok(());
                                }
                                Err(_) => {
                                    debug!("TCP transport negotiation timed out");
                                    return Ok(());
                                }
                            };
                            trace!("New tcp connection accepted");
                            let connection = graceful_shutdown.watch(
                                builder
                                    .serve_connection(TokioIo::new(stream), service)
                                    .into_owned(),
                            );
                            serve_connection(connection).await
                        }.instrument(socket_span))?;
                    },
                    Either::Right(unix_stream) => {
                        let io = TokioIo::new(unix_stream);
                        let connection = graceful_shutdown.watch(builder
                            .serve_connection(io, service.clone()).into_owned());
                        TaskCenter::spawn(TaskKind::SocketHandler, task_name.clone(), async move {
                            trace!("New uds connection accepted");
                            serve_connection(connection).await
                        }.instrument(socket_span))?;
                    }
                }
            }
        }
    }

    debug!("Draining current connections");
    tokio::select! {
        () = graceful_shutdown.shutdown() => {
            debug!("All connections completed gracefully");

        },
        () = tokio::time::sleep(Duration::from_secs(30)) => {
            info!("Some connections are taking longer to drain, dropping them");
        }
    }

    Ok(())
}

/// Upper bound on the TLS protocol sniff + handshake for a newly accepted
/// TCP connection. This runs in the per-connection task, so it bounds resource
/// usage per connection rather than protecting the accept loop.
const TLS_NEGOTIATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Establishes the application-layer transport on a freshly accepted TCP
/// stream: performs the TLS handshake in `require` mode; in `allow`/`prefer`
/// modes, sniffs the first byte (0x16 = TLS ClientHello) to decide between TLS
/// and plaintext; passes plaintext through otherwise.
async fn negotiate_tcp_transport(
    tcp_stream: tokio::net::TcpStream,
    tls_config: Option<TlsServerConfig>,
    tls_mode: TlsMode,
) -> Result<
    Either<tokio_rustls::server::TlsStream<tokio::net::TcpStream>, tokio::net::TcpStream>,
    Error,
> {
    let acceptor = match (&tls_config, &tls_mode) {
        (Some(config), TlsMode::Require) => Some(config.tls_acceptor()),
        (None, TlsMode::Require) => {
            return Err(Error::Configuration(
                "TLS is in required mode, but no TLS configuration is present. Rejecting the connection.".to_owned(),
            ));
        }
        (Some(config), TlsMode::Allow | TlsMode::Prefer) => {
            // Note: This is a poor man's implementation for TLS sniffing. It only works for
            // for sniffing TLS when the protocol used is HTTP as its first byte won't be 0x16 (the TLS ClientHello).
            // If we're to ever to switch to a different plaintext protocol, we'll need to implement a proper sniffer.
            let mut peek_buf = [0u8; 1];
            if matches!(tcp_stream.peek(&mut peek_buf).await, Ok(1) if peek_buf[0] == 0x16) {
                Some(config.tls_acceptor())
            } else {
                None
            }
        }
        _ => None,
    };

    match acceptor {
        Some(acceptor) => Ok(Either::Left(acceptor.accept(tcp_stream).await?)),
        None => Ok(Either::Right(tcp_stream)),
    }
}

async fn serve_connection(
    connection: impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
) -> Result<(), anyhow::Error> {
    if let Err(e) = connection.await {
        if let Some(hyper_error) = e.downcast_ref::<hyper::Error>() {
            if hyper_error.is_incomplete_message() {
                debug!("Connection closed before request completed");
            }
        } else {
            debug!("Connection terminated due to error: {e}");
        }
    } else {
        trace!("Connection completed cleanly");
    }
    Ok(())
}

#[derive(Clone, Default)]
struct TaskCenterExecutor;

impl<F> hyper::rt::Executor<F> for TaskCenterExecutor
where
    F: Future + 'static + Send,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        let _ = TaskCenter::spawn_child(TaskKind::H2ServerStream, "h2stream", async move {
            // ignore the future output
            let _ = fut.await;
            Ok(())
        });
    }
}

/// RandomAddressResolver is adapted from the default GaiResolver used in hyper_util:
/// https://github.com/hyperium/hyper-util/blob/v0.1.18/src/client/legacy/connect/dns.rs#L44
/// But instead of returning the full list of resolved ips in the order from getaddressinfo,
/// we choose a single random ipv4 and ipv6.
/// This allows us to handle headless initial addresses much better, as even if the dns server returns a random
/// address order, gai tends to reorder based on some proximity heuristics which mean we will never
/// hit all the IPs; resolving 100 times may have the same ip as the first returned every time.
#[derive(Clone)]
struct RandomAddressResolver;

pub struct RandomAddressResolverFuture<R>(JoinHandle<Result<R, io::Error>>);

impl tower::Service<hyper_util::client::legacy::connect::dns::Name> for RandomAddressResolver {
    type Response = std::iter::Chain<
        std::option::IntoIter<std::net::SocketAddr>,
        std::option::IntoIter<std::net::SocketAddr>,
    >;
    type Error = io::Error;
    type Future = RandomAddressResolverFuture<Self::Response>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: hyper_util::client::legacy::connect::dns::Name) -> Self::Future {
        RandomAddressResolverFuture(tokio::task::spawn_blocking(move || {
            use rand::seq::IteratorRandom;

            let addrs: Vec<_> =
                std::net::ToSocketAddrs::to_socket_addrs(&(name.as_str(), 0))?.collect();

            // the http connector cares about whether the first ip is ipv4 or ipv6 for the purposes of happy eyeballs
            // ie, if the first ip is v6, it will prefer v6
            let first_ipv4 = addrs.first().map(|addr| addr.is_ipv4()).unwrap_or(true);

            let ipv4s = addrs.iter().filter(|addr| addr.is_ipv4());
            let ipv6s = addrs.iter().filter(|addr| addr.is_ipv6());

            let rand = &mut rand::rng();
            let random_ipv4 = ipv4s.choose(rand).cloned();
            let random_ipv6 = ipv6s.choose(rand).cloned();

            if first_ipv4 {
                Ok(random_ipv4.into_iter().chain(random_ipv6))
            } else {
                Ok(random_ipv6.into_iter().chain(random_ipv4))
            }
        }))
    }
}

impl<R> Future for RandomAddressResolverFuture<R> {
    type Output = Result<R, io::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|res| match res {
            Ok(Ok(addrs)) => Ok(addrs),
            Ok(Err(err)) => Err(err),
            Err(join_err) => Err(io::Error::new(io::ErrorKind::Interrupted, join_err)),
        })
    }
}
