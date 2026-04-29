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
mod metric_definitions;
#[cfg(any(test, feature = "test_util"))]
pub mod test_util;
pub mod tls;

use std::{
    fmt::Display,
    io::{self, ErrorKind},
    net::IpAddr,
    str::FromStr,
    sync::{Arc, Weak},
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use dashmap::DashMap;
use futures::future::{BoxFuture, poll_fn};
use http::{Response, Uri};
use http_body::Body;
use metrics::histogram;
use restate_types::time::MillisSinceEpoch;
use rustls::pki_types::{DnsName, ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tower::Service;
use tracing::{debug, trace};

use crate::pool::{
    authority::AuthorityPool, conn::PermittedRecvStream,
    metric_definitions::CONNECTION_POOL_ACQUIRE_STREAM_DURATION,
};

pub use config::PoolBuilder;
use config::PoolConfig;
pub use conn::ConnectionError;

/// Errors that can occur during the lifecycle of an H2 connection.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IO(#[from] io::Error),
    #[error(transparent)]
    H2(#[from] h2::Error),
    #[error("connection is closed")]
    Closed,
    #[error("connection keep-alive timeout")]
    KeepAliveTimeout,
}

#[derive(Clone)]
pub struct Pool<C> {
    connector: C,
    config: PoolConfig,
    authorities: Arc<DashMap<PoolKey, AuthorityPool<C>>>,
}

impl<C: Clone + Send + Sync + 'static> Pool<C> {
    fn new(connector: C, config: PoolConfig) -> Self {
        metric_definitions::describe_metrics();

        let authorities = Arc::new(DashMap::default());

        if let Some(idle_timeout) = config.idle_authority_timeout {
            tokio::task::Builder::new()
                .name("h2:eviction-task")
                .spawn(eviction_task(Arc::downgrade(&authorities), idle_timeout))
                .unwrap();
        }

        Self {
            config,
            connector,
            authorities,
        }
    }
}

impl<C> Pool<C>
where
    C: Service<Uri> + Send + Sync + Clone + 'static,
    C::Response: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    C::Future: Send + 'static,
    C::Error: Into<Error>,
{
    pub fn request<B>(
        &self,
        request: http::Request<B>,
    ) -> impl Future<Output = Result<Response<PermittedRecvStream>, Error>> + Send + 'static
    where
        B: Body<Data = Bytes> + Unpin + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        trace!("(h2 pool) requesting ({})", request.uri());
        let key = PoolKey::from_uri(request.uri());

        let mut authority_pool = self
            .authorities
            .entry(key)
            .or_insert_with(|| AuthorityPool::new(self.connector.clone(), self.config))
            .value()
            .clone();
        authority_pool.touch();

        async move {
            let mut request = request;
            loop {
                let start_time = MillisSinceEpoch::now();
                poll_fn(|cx| authority_pool.poll_ready(cx)).await?;
                histogram!(CONNECTION_POOL_ACQUIRE_STREAM_DURATION).record(start_time.elapsed());

                match authority_pool.call(request).await {
                    Ok(result) => return Ok(result),
                    Err(conn::ConnectionError::Error(err)) => return Err(err),
                    Err(conn::ConnectionError::PermitReclaimed(req)) => {
                        debug!("H2 request lost stream permit, retrying");
                        request = req;
                    }
                }
            }
        }
    }
}

/// Background task that periodically evicts idle authority pools.
///
/// Eviction uses two phases because simply removing a pool from the map and
/// dropping it would close the underlying H2 connections immediately — aborting
/// any requests that are still in flight. By splitting the process we decouple
/// "stop routing new requests here" from "shut down the connections":
///
/// 1. **Evict** — Remove idle pools from the active [`DashMap`] into a
///    task-local drain list. New requests to the same authority will create a
///    fresh pool, while existing in-flight requests continue on the evicted
///    connection until all permits are dropped (has_inflight() is false).
/// 2. **Drain** — On each subsequent tick, check whether every connection in a
///    draining pool has zero in-flight streams. Once that is true the pool is
///    dropped, which triggers [`AuthorityPoolInner::Drop`] and gracefully
///    closes all connections.
///
/// The task exits when all [`Pool`] clones are dropped (weak upgrade fails)
/// **and** the drain list is empty.
async fn eviction_task<C: Clone>(
    authorities: Weak<DashMap<PoolKey, AuthorityPool<C>>>,
    idle_timeout: Duration,
) {
    let interval = (idle_timeout / 4).max(Duration::from_secs(10));
    let mut draining: Vec<AuthorityPool<C>> = Vec::new();

    loop {
        tokio::time::sleep(interval).await;

        // Phase 2: drop drained pools that have no in-flight streams.
        draining.retain(|pool| pool.has_inflight());

        // Phase 1: evict idle pools from the active map.
        match authorities.upgrade() {
            Some(map) => {
                let now = MillisSinceEpoch::now();
                map.retain(|key, pool| {
                    if now.duration_since(pool.last_used()) > idle_timeout {
                        trace!("evicting idle authority pool ({})", key);
                        draining.push(pool.clone());
                        false
                    } else {
                        true
                    }
                });
            }
            None => {
                // All Pool clones dropped. Keep draining until empty, then exit.
                if draining.is_empty() {
                    return;
                }
            }
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
struct PoolKey {
    scheme: Option<http::uri::Scheme>,
    authority: Option<http::uri::Authority>,
}

impl Display for PoolKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.scheme {
            Some(schema) => write!(f, "{}", schema)?,
            None => write!(f, "unknown")?,
        }

        match &self.authority {
            Some(authority) => write!(f, "://{}", authority)?,
            None => write!(f, "://unknown")?,
        }

        Ok(())
    }
}

impl PoolKey {
    fn from_uri(u: &Uri) -> Self {
        Self {
            scheme: u.scheme().cloned(),
            authority: u.authority().cloned(),
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Schema {
    Unknown,
    Http,
    Https,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConnectionInfo {
    schema: Schema,
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

    pub fn schema(&self) -> Schema {
        self.schema
    }
}

impl IntoConnectionInfo for Uri {
    fn get_connection_info(&self) -> ConnectionInfo {
        let (schema, default_port) = match self.scheme() {
            None => (Schema::Unknown, None),
            Some(schema) => match schema.as_str() {
                "http" => (Schema::Http, Some(80)),
                "https" => (Schema::Https, Some(443)),
                _ => (Schema::Unknown, None),
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

        ConnectionInfo { schema, host, port }
    }
}

impl<T> IntoConnectionInfo for http::Request<T> {
    fn get_connection_info(&self) -> ConnectionInfo {
        self.uri().get_connection_info()
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use bytes::Bytes;
    use http::{Request, Uri};
    use http_body_util::BodyExt;
    use restate_types::time::MillisSinceEpoch;

    use crate::pool::PoolBuilder;
    use crate::pool::test_util::TestConnector;

    fn make_pool(max_concurrent_streams: u32) -> super::Pool<TestConnector> {
        PoolBuilder::default()
            .initial_max_send_streams(std::num::NonZeroU32::new(max_concurrent_streams).unwrap())
            .build(TestConnector::new(max_concurrent_streams))
    }

    fn make_pool_with_eviction(
        max_concurrent_streams: u32,
        idle_timeout: Duration,
    ) -> super::Pool<TestConnector> {
        PoolBuilder::default()
            .initial_max_send_streams(std::num::NonZeroU32::new(max_concurrent_streams).unwrap())
            .idle_authority_timeout(Some(idle_timeout))
            .build(TestConnector::new(max_concurrent_streams))
    }

    /// Requests to different hosts create separate authority pools.
    #[tokio::test]
    async fn routes_to_separate_authorities() {
        let pool = make_pool(10);

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
        let pool = make_pool(10);

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
        let pool = make_pool(10);

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

    /// Idle authority pools are evicted by the background task.
    /// Active pools (recently touched) are retained.
    #[tokio::test]
    async fn evicts_idle_authority_pools() {
        let idle_timeout = Duration::from_secs(10);
        let pool = make_pool_with_eviction(10, idle_timeout);

        // Send requests to two authorities to create their pools.
        for host in ["host-a", "host-b"] {
            pool.request(
                Request::builder()
                    .uri(format!("http://{}:80", host))
                    .body(http_body_util::Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        }
        assert_eq!(pool.authorities.len(), 2);

        // Backdate host-a's last_used to simulate it being idle longer than the timeout.
        let stale = MillisSinceEpoch::from(MillisSinceEpoch::now().as_u64().saturating_sub(20_000));
        pool.authorities
            .iter()
            .find(|entry| {
                entry
                    .key()
                    .authority
                    .as_ref()
                    .is_some_and(|a| a.as_str() == "host-a:80")
            })
            .unwrap()
            .value()
            .set_last_used(stale);

        // Run eviction directly to avoid timing dependencies.
        let now = MillisSinceEpoch::now();
        pool.authorities
            .retain(|_key, ap| now.duration_since(ap.last_used()) <= idle_timeout);

        // host-a should be evicted, host-b should remain.
        assert_eq!(pool.authorities.len(), 1);
        assert!(pool.authorities.iter().any(|e| {
            e.key()
                .authority
                .as_ref()
                .is_some_and(|a| a.as_str() == "host-b:80")
        }));

        // A new request to host-a creates a fresh pool.
        pool.request(
            Request::builder()
                .uri("http://host-a:80")
                .body(http_body_util::Empty::<Bytes>::new())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(pool.authorities.len(), 2);
    }

    /// Evicted authority pools with in-flight streams are kept in the drain
    /// list until streams complete, then dropped (closing connections via Drop).
    #[tokio::test]
    async fn draining_waits_for_inflight_streams() {
        let pool = make_pool_with_eviction(10, Duration::from_secs(10));

        // Send a request and hold the response body to keep the stream alive.
        let body = pool
            .request(
                Request::builder()
                    .uri("http://host-a:80")
                    .body(http_body_util::Full::new(Bytes::from_static(b"hello")))
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body();

        assert_eq!(pool.authorities.len(), 1);

        // Clone the authority pool before removing it (simulates what eviction_task does).
        let draining_pool = pool.authorities.iter().next().unwrap().value().clone();

        // Remove from DashMap.
        pool.authorities.clear();
        assert_eq!(pool.authorities.len(), 0);

        // The draining pool should report in-flight streams.
        assert!(draining_pool.has_inflight());

        // Drop the response body to complete the stream.
        drop(body);

        // Now the draining pool should have no in-flight streams.
        assert!(!draining_pool.has_inflight());
    }
}
