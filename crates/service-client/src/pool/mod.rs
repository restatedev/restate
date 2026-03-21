// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod authority;
mod config;
pub mod conn;
pub mod tls;

use std::{
    future::poll_fn,
    hash::{Hash, Hasher},
    io::{self, ErrorKind},
    net::IpAddr,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use dashmap::DashMap;
use futures::future::BoxFuture;
use http::{Response, Uri};
use http_body::Body;
use rustls::pki_types::{DnsName, ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tower::Service;
use tracing::trace;

use crate::pool::{authority::AuthorityPool, conn::PermittedRecvStream};

pub use config::PoolBuilder;
use config::PoolConfig;
pub use conn::ConnectionError;

#[derive(Clone)]
pub struct Pool<C> {
    connector: C,
    config: PoolConfig,
    authorities: Arc<DashMap<u64, AuthorityPool<C>>>,
}

impl<C> Pool<C> {
    fn new(connector: C, config: PoolConfig) -> Self {
        Self {
            config,
            connector,
            authorities: Arc::new(DashMap::default()),
        }
    }
}

impl<C> Pool<C>
where
    C: Service<Uri> + Send + Clone + 'static,
    C::Response: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    C::Future: Send + 'static,
    C::Error: Into<ConnectionError>,
{
    pub fn request<B>(
        &self,
        request: http::Request<B>,
    ) -> impl Future<Output = Result<Response<PermittedRecvStream>, ConnectionError>> + Send + 'static
    where
        B: Body<Data = Bytes> + Unpin + Send + Sync + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        let extractor = KeyExtractor::from_uri(request.uri());

        let mut authority_pool = self
            .authorities
            .entry(extractor.key())
            .or_insert_with(|| AuthorityPool::new(self.connector.clone(), self.config.clone()))
            .value()
            .clone();

        async move {
            poll_fn(|cx| authority_pool.poll_ready(cx)).await?;
            authority_pool.call(request).await
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
struct KeyExtractor<'a> {
    scheme: Option<&'a http::uri::Scheme>,
    authority: Option<&'a http::uri::Authority>,
}

impl<'a> KeyExtractor<'a> {
    fn from_uri(u: &'a Uri) -> Self {
        Self {
            scheme: u.scheme(),
            authority: u.authority(),
        }
    }

    fn key(self) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

/// A Tower [`Service`] that establishes TCP connections to a given URI.
///
/// Extracts the host and port from the URI (defaulting to port 80 for HTTP
/// and 443 for HTTPS) and connects via [`TcpStream`].
#[derive(Debug, Clone, Copy)]
pub struct TcpConnector {
    connect_timeout: Duration,
}

impl TcpConnector {
    pub fn new(connect_timeout: Duration) -> Self {
        Self { connect_timeout }
    }
}

impl Service<Uri> for TcpConnector {
    type Response = TcpStream;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let connect_timeout = self.connect_timeout;
        let fut = async move {
            let req = req.get_connection_info();
            trace!("connecting to {:?}:{:?}", req.host, req.port());

            let host = req
                .host()
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "unknown host name"))?;
            let port = req
                .port()
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "missing port number"))?;

            let stream = tokio::time::timeout(connect_timeout, async {
                match host {
                    Host::IpAddress(addr) => TcpStream::connect((*addr, port)).await,
                    Host::DnsName(dns) => TcpStream::connect((dns.as_ref(), port)).await,
                }
            })
            .await
            .map_err(|_| io::Error::new(ErrorKind::TimedOut, "connect timeout"))??;

            stream.set_nodelay(true)?;
            Ok(stream)
        };

        Box::pin(fut)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Host {
    IpAddress(IpAddr),
    DnsName(DnsName<'static>),
}

impl From<Host> for ServerName<'static> {
    fn from(value: Host) -> Self {
        match value {
            Host::IpAddress(addr) => ServerName::IpAddress(addr.into()),
            Host::DnsName(dns) => ServerName::DnsName(dns),
        }
    }
}

trait IntoConnectionInfo {
    fn get_connection_info(&self) -> ConnectionInfo;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConnectionInfo {
    secure: Option<bool>,
    host: Option<Host>,
    port: Option<u16>,
}

impl ConnectionInfo {
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    pub fn host(&self) -> Option<&Host> {
        self.host.as_ref()
    }

    pub fn secure(&self) -> Option<bool> {
        self.secure
    }
}

impl IntoConnectionInfo for Uri {
    fn get_connection_info(&self) -> ConnectionInfo {
        let (secure, default_port) = match self.scheme() {
            None => (None, None),
            Some(schema) => match schema.as_str() {
                "http" => (Some(false), Some(80)),
                "https" => (Some(true), Some(443)),
                _ => (None, None),
            },
        };

        let port = self.port_u16().or(default_port);
        let host = match self.host() {
            None => None,
            Some(host) => match std::net::IpAddr::from_str(host) {
                Ok(addr) => Some(Host::IpAddress(addr)),
                Err(_) => DnsName::try_from_str(host)
                    .ok()
                    .map(|x| Host::DnsName(x.to_owned())),
            },
        };

        ConnectionInfo { secure, host, port }
    }
}

impl<T> IntoConnectionInfo for http::Request<T> {
    fn get_connection_info(&self) -> ConnectionInfo {
        self.uri().get_connection_info()
    }
}

#[cfg(test)]
mod test {
    use std::io;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use futures::future::BoxFuture;
    use http::{Request, StatusCode, Uri};
    use http_body_util::BodyExt;
    use tokio::io::DuplexStream;
    use tower::Service;

    use crate::pool::PoolBuilder;

    struct ServerConfig {
        max_concurrent_streams: u32,
    }

    #[derive(Clone)]
    struct TestConnector {
        config: Arc<ServerConfig>,
    }

    impl TestConnector {
        fn new(max_concurrent_streams: u32) -> Self {
            Self {
                config: Arc::new(ServerConfig {
                    max_concurrent_streams,
                }),
            }
        }
    }

    impl Service<Uri> for TestConnector {
        type Response = DuplexStream;
        type Error = io::Error;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Uri) -> Self::Future {
            let config = Arc::clone(&self.config);
            Box::pin(async move {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(run_server(server, config));
                Ok(client)
            })
        }
    }

    async fn run_server(stream: DuplexStream, config: Arc<ServerConfig>) {
        let mut h2 = h2::server::Builder::new()
            .max_concurrent_streams(config.max_concurrent_streams)
            .handshake::<_, Bytes>(stream)
            .await
            .unwrap();

        while let Some(request) = h2.accept().await {
            let (request, mut respond) = request.unwrap();
            tokio::spawn(async move {
                let response = http::Response::builder()
                    .status(StatusCode::OK)
                    .body(())
                    .unwrap();
                let mut send_stream = respond.send_response(response, false).unwrap();
                let mut recv_body = request.into_body();

                while let Some(data) = recv_body.data().await {
                    let data = data.unwrap();
                    recv_body
                        .flow_control()
                        .release_capacity(data.len())
                        .unwrap();

                    send_stream.reserve_capacity(data.len());
                    let _ = futures::future::poll_fn(|cx| send_stream.poll_capacity(cx)).await;
                    if send_stream.send_data(data, false).is_err() {
                        return;
                    }
                }

                let _ = send_stream.send_trailers(http::HeaderMap::new());
            });
        }
    }

    fn make_pool(
        max_concurrent_streams: u32,
        max_connections: usize,
    ) -> super::Pool<TestConnector> {
        PoolBuilder::default()
            .max_connections(std::num::NonZeroUsize::new(max_connections).unwrap())
            .initial_max_send_streams(std::num::NonZeroU32::new(max_concurrent_streams).unwrap())
            .build(TestConnector::new(max_concurrent_streams))
    }

    /// Requests to different hosts create separate authority pools.
    #[tokio::test]
    async fn routes_to_separate_authorities() {
        let pool = make_pool(10, 4);

        assert_eq!(pool.authorities.len(), 0);

        pool.request(
            Request::builder()
                .uri("http://host-a:80")
                .body(http_body_util::Empty::<Bytes>::new())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(pool.authorities.len(), 1);

        pool.request(
            Request::builder()
                .uri("http://host-b:80")
                .body(http_body_util::Empty::<Bytes>::new())
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(pool.authorities.len(), 2);
    }

    /// Multiple requests to the same authority reuse the same pool entry.
    #[tokio::test]
    async fn same_authority_shares_pool() {
        let pool = make_pool(10, 4);

        for _ in 0..3 {
            pool.request(
                Request::builder()
                    .uri("http://host-a:80")
                    .body(http_body_util::Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        }

        assert_eq!(pool.authorities.len(), 1);
    }

    /// Requests to multiple authorities with echo payloads all resolve correctly.
    #[tokio::test]
    async fn multi_authority_echo() {
        let pool = make_pool(10, 4);

        for (i, host) in ["host-a", "host-b", "host-c"].iter().enumerate() {
            let uri: Uri = format!("http://{}:80", host).parse().unwrap();
            let resp = pool
                .request(
                    Request::builder()
                        .uri(uri)
                        .body(http_body_util::Full::new(Bytes::from(vec![i as u8; 4])))
                        .unwrap(),
                )
                .await
                .unwrap();

            let collected = resp.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(
                collected.as_ref(),
                &[i as u8; 4],
                "response should echo request body for {}",
                host,
            );
        }

        assert_eq!(pool.authorities.len(), 3);
    }
}
