// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
mod authority;
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
use rustls::pki_types::{DnsName, ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tower::Service;
use tracing::{debug, trace};

use restate_types::time::MillisSinceEpoch;

use crate::pool::{
    authority::AuthorityPool,
    conn::{Connection, PermittedRecvStream},
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
    H2(h2::Error),
    #[error(
        "deployment generating many small data frames. Please adjust `worker.invoker.http2-data-frame-budget` according to your workload shape"
    )]
    TooManyDataFrames,
    #[error("connection is closed")]
    Closed,
    #[error("connection keep-alive timeout")]
    KeepAliveTimeout,
}

impl From<h2::Error> for Error {
    fn from(err: h2::Error) -> Self {
        match err.reason() {
            Some(reason)
                if reason == h2::Reason::ENHANCE_YOUR_CALM
                    && err.to_string().contains("too_many_data_frames") =>
            {
                Self::TooManyDataFrames
            }
            _ => Self::H2(err),
        }
    }
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

        let policy = EvictionPolicy::from_config(&config);
        if policy.is_enabled() {
            tokio::task::Builder::new()
                .name("h2:eviction-task")
                .spawn(eviction_task(Arc::downgrade(&authorities), policy))
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
    C::Response: AsyncRead + AsyncWrite + std::fmt::Debug + Unpin + Send + Sync + 'static,
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

        async move {
            let mut request = request;
            loop {
                let start_time = MillisSinceEpoch::now();
                poll_fn(|cx| authority_pool.poll_ready(cx)).await?;
                histogram!(CONNECTION_POOL_ACQUIRE_STREAM_DURATION).record(start_time.elapsed());
                match authority_pool.call(request).await {
                    Ok(result) => return Ok(result),
                    Err(conn::ConnectionError::Error(err)) => return Err(err),
                    Err(conn::ConnectionError::Retry(req, reason)) => {
                        debug!(reason, "H2 retrying unsent request");
                        request = req;
                    }
                }
            }
        }
    }
}

/// Why the eviction task removed a connection from its authority pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvictReason {
    /// No in-flight streams and `last_used_at` older than the idle timeout.
    Idle,
    /// `created_at` older than the (jittered) max connection age. In-flight
    /// streams finish on the retired connection; new requests get a fresh one.
    Aged,
}

/// Rules applied by [`eviction_task`] on every tick.
#[derive(Debug, Clone, Copy)]
struct EvictionPolicy {
    idle_timeout: Option<Duration>,
    max_age: Option<Duration>,
    max_age_jitter: f32,
}

impl EvictionPolicy {
    fn from_config(config: &PoolConfig) -> Self {
        Self {
            idle_timeout: config.idle_connection_timeout,
            max_age: config.max_connection_age,
            max_age_jitter: config.max_connection_age_jitter.clamp(0.0, 1.0),
        }
    }

    fn is_enabled(&self) -> bool {
        self.idle_timeout.is_some() || self.max_age.is_some()
    }

    /// How often the eviction task wakes up: a quarter of the shortest
    /// configured timeout, but never more often than every 10 seconds.
    fn interval(&self) -> Duration {
        let shortest = [self.idle_timeout, self.max_age]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(Duration::from_secs(10));
        (shortest / 4).max(Duration::from_secs(10))
    }

    /// Returns why `con` should leave the pool at `now`, or `None` to keep it.
    fn evict_reason<C>(&self, con: &Connection<C>, now: MillisSinceEpoch) -> Option<EvictReason> {
        if let Some(max_age) = self.max_age {
            let max_age = jittered_age(max_age, self.max_age_jitter, con.id());
            if now.duration_since(con.created_at()) >= max_age {
                return Some(EvictReason::Aged);
            }
        }

        if let Some(idle_timeout) = self.idle_timeout
            && con.inflight() == 0
            && now.duration_since(con.last_used_at()) >= idle_timeout
        {
            return Some(EvictReason::Idle);
        }

        None
    }
}

/// Spreads `max_age` by up to `jitter` (a fraction of `max_age`) using the
/// connection id, so the same connection always sees the same deadline and
/// connections opened together retire on different ticks.
fn jittered_age(max_age: Duration, jitter: f32, connection_id: usize) -> Duration {
    if jitter <= 0.0 {
        return max_age;
    }
    let spread = (connection_id % 1000) as f32 / 1000.0;
    max_age.saturating_add(max_age.mul_f32(jitter * spread))
}

/// Background task that periodically evicts idle and aged connections.
///
/// On each tick the task walks every authority pool and removes connections
/// that are either:
///
/// - idle: `inflight() == 0` **and** `last_used_at` is older than
///   `idle_timeout`. Connections with in-flight streams are left in place so
///   that ongoing requests — including long-lived streams whose `last_used_at`
///   has gone stale — continue to be reused by new callers; or
/// - aged: `created_at` is older than `max_connection_age` (plus a stable
///   per-connection jitter), regardless of in-flight streams. Removing the
///   connection from the pool stops new streams from being opened on it,
///   while in-flight streams keep it alive until they complete (see below).
///   The next request opens a fresh connection, which lets an L4 load
///   balancer re-spread connections across the backends available now.
///
/// Authority pools whose connection list becomes empty are removed from the
/// [`DashMap`].
///
/// Evicted connections do not need to be tracked here: each in-flight
/// [`PermittedRecvStream`] holds an `Arc<ConnectionShared>`, so the
/// underlying H2 handle stays alive until the last outstanding stream
/// completes. This also covers the race in [`AuthorityPool::poll_ready`]
/// where a caller clones a `Connection` under the read lock and acquires a
/// stream permit on it after the lock is released — even if the eviction
/// task removes that connection in between, the late-arriving request still
/// has a live H2 handle to use, and graceful close via
/// [`conn::Connection`]'s `Drop` fires once the last `Arc` is released.
///
/// The task exits as soon as all [`Pool`] clones are dropped (weak upgrade
/// fails).
async fn eviction_task<C: Clone>(
    authorities: Weak<DashMap<PoolKey, AuthorityPool<C>>>,
    policy: EvictionPolicy,
) {
    let interval = policy.interval();

    loop {
        tokio::time::sleep(interval).await;

        // Evict idle and aged connections from each authority pool.
        match authorities.upgrade() {
            Some(map) => {
                let now = MillisSinceEpoch::now();
                map.retain(|key, pool| {
                    let mut idle = 0usize;
                    let mut aged = 0usize;
                    let (size, _evicted) = pool.retain(|con| match policy.evict_reason(con, now) {
                        None => true,
                        Some(EvictReason::Idle) => {
                            idle += 1;
                            false
                        }
                        Some(EvictReason::Aged) => {
                            aged += 1;
                            false
                        }
                    });

                    if idle > 0 {
                        debug!("evicting {idle} idle connections from pool ({key})");
                    }

                    if aged > 0 {
                        debug!(
                            "retiring {aged} connections past max age from pool ({key}); \
                             in-flight streams drain before close"
                        );
                    }

                    if size == 0 {
                        debug!("evicting empty authority pool ({})", key);
                    }
                    size != 0
                });
            }
            None => {
                return;
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

    use crate::pool::test_util::TestConnector;
    use crate::pool::{EvictReason, EvictionPolicy, PoolBuilder, jittered_age};

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
            .idle_connection_timeout(Some(idle_timeout))
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

    /// A connection removed from the authority pool keeps reporting its
    /// in-flight count for as long as the response body is held, and the
    /// count drops to zero once the body is released — the permit (and the
    /// `Arc<ConnectionShared>` inside [`PermittedRecvStream`]) is what
    /// keeps the stream slot occupied past eviction.
    #[tokio::test]
    async fn inflight_reflects_body_after_eviction() {
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

        // Drain all connections
        let (remaining, draining_pool) = pool
            .authorities
            .iter()
            .next()
            .unwrap()
            .value()
            .retain(|_| false);

        assert_eq!(remaining, 0);
        // Remove from DashMap.
        pool.authorities.clear();
        assert_eq!(pool.authorities.len(), 0);

        // The draining pool should report in-flight streams.
        assert_ne!(draining_pool.iter().fold(0, |acc, v| acc + v.inflight()), 0);

        // Drop the response body to complete the stream.
        drop(body);

        // Now the draining pool should have no in-flight streams.
        assert_eq!(draining_pool.iter().fold(0, |acc, v| acc + v.inflight()), 0);
    }

    #[test]
    fn jittered_age_is_stable_and_bounded() {
        let age = Duration::from_secs(600);

        // No jitter: exact age for any connection.
        assert_eq!(jittered_age(age, 0.0, 7), age);

        // Stable per connection id, bounded by age * (1 + jitter).
        for id in [0usize, 1, 499, 999, 1000, 123_456] {
            let a = jittered_age(age, 0.1, id);
            let b = jittered_age(age, 0.1, id);
            assert_eq!(a, b, "jitter must be deterministic per connection id");
            assert!(a >= age, "jitter must never shorten the age");
            assert!(
                a <= age.mul_f32(1.1),
                "jitter must stay within the fraction"
            );
        }

        // Ids that differ spread out over the window.
        assert_ne!(jittered_age(age, 0.1, 1), jittered_age(age, 0.1, 999));
    }

    #[test]
    fn eviction_interval_follows_shortest_timeout() {
        let policy = |idle: Option<u64>, age: Option<u64>| EvictionPolicy {
            idle_timeout: idle.map(Duration::from_secs),
            max_age: age.map(Duration::from_secs),
            max_age_jitter: 0.0,
        };

        assert!(!policy(None, None).is_enabled());
        assert!(policy(Some(300), None).is_enabled());
        assert!(policy(None, Some(300)).is_enabled());

        // idle only: 300s / 4
        assert_eq!(policy(Some(300), None).interval(), Duration::from_secs(75));
        // age only: 120s / 4
        assert_eq!(policy(None, Some(120)).interval(), Duration::from_secs(30));
        // both: the shorter one wins
        assert_eq!(
            policy(Some(300), Some(120)).interval(),
            Duration::from_secs(30)
        );
        // never more often than every 10s
        assert_eq!(policy(None, Some(4)).interval(), Duration::from_secs(10));
    }

    /// A connection past `max_connection_age` is retired even while it has
    /// in-flight streams: it leaves the pool (so the next request opens a new
    /// connection) but the held stream keeps working until it is released.
    /// Idle eviction alone never touches a connection with in-flight streams.
    #[tokio::test]
    async fn aged_connection_is_retired_while_inflight_streams_drain() {
        let pool = make_pool(10);

        // Open a connection and hold the response body to keep a stream in flight.
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

        let authority = pool.authorities.iter().next().unwrap().value().clone();
        let now = MillisSinceEpoch::now();

        // Idle-only policy: the connection is busy, so it stays.
        let idle_only = EvictionPolicy {
            idle_timeout: Some(Duration::ZERO),
            max_age: None,
            max_age_jitter: 0.0,
        };
        let (remaining, evicted) =
            authority.retain(|con| idle_only.evict_reason(con, now).is_none());
        assert_eq!(remaining, 1);
        assert!(evicted.is_empty());

        // Age policy with a zero age: retired despite the in-flight stream.
        let aged = EvictionPolicy {
            idle_timeout: None,
            max_age: Some(Duration::ZERO),
            max_age_jitter: 0.0,
        };
        let (remaining, evicted) = authority.retain(|con| aged.evict_reason(con, now).is_none());
        assert_eq!(remaining, 0);
        assert_eq!(evicted.len(), 1);
        let retired = &evicted[0];
        assert_eq!(aged.evict_reason(retired, now), Some(EvictReason::Aged));
        assert_eq!(retired.inflight(), 1, "held stream keeps the slot occupied");

        // A new request after retirement lands on a brand-new connection.
        let resp = pool
            .request(
                Request::builder()
                    .uri("http://host-a:80")
                    .body(http_body_util::Full::new(Bytes::from_static(b"again")))
                    .unwrap(),
            )
            .await
            .unwrap();
        let echoed = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(echoed.as_ref(), b"again");

        let (fresh, _) = authority.retain(|con| {
            assert_ne!(
                con.id(),
                retired.id(),
                "retired connection must not be reused"
            );
            true
        });
        assert_eq!(fresh, 1);

        // Releasing the held body completes the stream on the retired connection.
        drop(body);
        assert_eq!(retired.inflight(), 0);
    }
}
