// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A pool of HTTP/2 connections to a single HTTP authority (scheme + host + port).
//!
//! [`AuthorityPool`] manages multiple [`Connection`] instances, creating new ones
//! on demand when existing connections are fully utilized, and evicting connections
//! that have failed.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use http::Uri;
use http_body::Body;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use restate_types::time::MillisSinceEpoch;
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Service;
use tracing::trace;

use crate::pool::PoolConfig;
use crate::pool::conn::{ConnectionConfig, ConnectionConfigBuilder};

use super::Error;
use super::conn::{Connection, ResponseFuture};

/// Shared mutable state for the pool.
struct AuthorityPoolInner<C> {
    epoch: usize,
    connections: Vec<Connection<C>>,
}

/// A pool of HTTP/2 connections to a single HTTP authority.
///
/// Manages multiple [`Connection`] instances, creating new ones on demand when
/// all existing connections are fully utilized (no available H2 streams), and
/// evicting connections that have entered a closed/failed state.
///
/// Cloning an `AuthorityPool` shares the underlying connection set; each clone
/// maintains its own per-handle state for the `poll_ready`/`call` cycle.
pub struct AuthorityPool<C> {
    connector: C,
    config: PoolConfig,
    connection_config: ConnectionConfig,
    inner: Arc<RwLock<AuthorityPoolInner<C>>>,
    /// Timestamp of the last request routed through this pool. Updated
    /// atomically without holding the inner lock.
    last_used: Arc<AtomicU64>,
    /// The readied connection (permit acquired). Consumed by [`call`].
    ready: Option<Connection<C>>,
    /// Connections being polled for readiness. When all connections are at
    /// capacity, we poll all of them so we're woken no matter which one
    /// frees up a stream.
    candidates: Vec<Connection<C>>,
}

impl<C: Clone> Clone for AuthorityPool<C> {
    fn clone(&self) -> Self {
        Self {
            connector: self.connector.clone(),
            config: self.config,
            connection_config: self.connection_config,
            inner: Arc::clone(&self.inner),
            last_used: Arc::clone(&self.last_used),
            ready: None,
            candidates: Vec::default(),
        }
    }
}

impl<C> AuthorityPool<C> {
    /// Updates the last-used timestamp to the current time.
    pub(crate) fn touch(&self) {
        self.last_used
            .store(MillisSinceEpoch::now().as_u64(), Ordering::Relaxed);
    }

    /// Returns the last-used timestamp.
    pub(crate) fn last_used(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::from(self.last_used.load(Ordering::Relaxed))
    }

    #[cfg(test)]
    pub(crate) fn set_last_used(&self, ts: MillisSinceEpoch) {
        self.last_used.store(ts.as_u64(), Ordering::Relaxed);
    }

    /// Returns `true` if any connection has in-flight H2 streams.
    pub(crate) fn has_inflight(&self) -> bool {
        self.inner
            .read()
            .connections
            .iter()
            .any(|c| c.inflight() > 0)
    }
}

impl<C> AuthorityPool<C>
where
    C: Service<Uri> + Clone,
    C::Response: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    C::Future: Send + 'static,
    C::Error: Into<Error>,
{
    pub fn new(connector: C, config: PoolConfig) -> Self {
        let connection_config = ConnectionConfigBuilder::default()
            .initial_max_send_streams(config.initial_max_send_streams.get())
            .streams_per_connection_limit(config.streams_per_connection_limit.get())
            .keep_alive_interval(config.keep_alive_interval)
            .keep_alive_timeout(config.keep_alive_timeout)
            .build()
            .unwrap();

        Self {
            connector,
            config,
            connection_config,
            inner: Arc::new(RwLock::new(AuthorityPoolInner {
                epoch: 0,
                connections: Vec::new(),
            })),
            last_used: Arc::new(AtomicU64::new(MillisSinceEpoch::now().as_u64())),
            ready: None,
            candidates: Vec::default(),
        }
    }

    /// Polls the pool for a connection with an available H2 stream permit.
    ///
    /// Returns `Ready(Ok(()))` once a permit has been acquired on one connection,
    /// after which [`call`](Self::call) may be invoked exactly once.
    ///
    /// The algorithm runs in a loop:
    /// 1. If a permit was already acquired in a previous poll, return immediately.
    /// 2. Poll all current candidates. Closed candidates are evicted. The first
    ///    to return `Ready(Ok)` wins and the candidate list is cleared. Candidates
    ///    that error are evicted. Pending candidates register wakers so we're
    ///    woken when any of them frees up a stream.
    /// 3. Expand the candidate set with connections from the shared pool that
    ///    aren't already candidates. Additionally, if the ratio of available H2
    ///    streams to total capacity falls below
    ///    [`eager_connection_threshold_percent`](PoolConfig::eager_connection_threshold_percent),
    ///    proactively create a new connection (capacity permitting). If candidates
    ///    were added, randomize their order for load balancing and loop back to
    ///    step 2.
    /// 4. If no new candidates were found, create a new connection (after
    ///    evicting closed ones from the shared pool) provided we're under
    ///    [`max_connections`](PoolConfig::max_connections), and poll it.
    /// 5. If at the connection limit with all candidates pending, return
    ///    `Pending`. Wakers were already registered in step 2.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.ready.is_some() {
            return Poll::Ready(Ok(()));
        }

        let mut pool_expanded = false;
        loop {
            let mut i = 0;
            let mut current_ids = HashSet::new();

            while i < self.candidates.len() {
                let candidate = &mut self.candidates[i];
                if candidate.is_closed() {
                    self.candidates.swap_remove(i);
                    continue;
                }

                match candidate.poll_ready(cx) {
                    Poll::Ready(Ok(_)) => {
                        self.ready = Some(self.candidates.swap_remove(i));
                        self.candidates.clear();
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(err)) => {
                        // keep trying other candidates. If all candidates
                        // have been evicted, we try the remaining connections
                        // in the pool (after eviction).
                        self.candidates.swap_remove(i);
                        trace!("connection evicted from the pool due to error: {err}")
                    }
                    Poll::Pending => {
                        // Waker registered inside conn.poll_ready — we'll
                        // be woken when this connection frees a stream.
                        current_ids.insert(candidate.id());
                        i += 1;
                    }
                }
            }

            // Remaining candidates are all pending (or the list is empty if
            // all errored). Try to expand with connections from the shared pool
            // that we haven't polled yet this iteration.
            let mut inner = self.inner.upgradable_read();

            let mut has_some_closed = false;
            let mut candidates_expanded = false;

            let mut total_available_streams = 0usize;
            let mut total_max_concurrent_streams = 0usize;

            for candidate in &inner.connections {
                if candidate.is_closed() {
                    has_some_closed = true;
                    continue;
                }

                total_available_streams =
                    total_available_streams.saturating_add(candidate.available_streams());
                total_max_concurrent_streams =
                    total_max_concurrent_streams.saturating_add(candidate.max_concurrent_streams());

                if current_ids.contains(&candidate.id()) {
                    continue;
                }

                candidates_expanded = true;
                self.candidates.push(candidate.clone());
            }

            let should_expand = if total_max_concurrent_streams == 0 {
                // No live connections — definitely create one
                true
            } else {
                self.config.connection_saturation_threshold.is_some_and(
                    |connection_saturation_threshold| {
                        let available_ratio =
                            total_available_streams as f64 / total_max_concurrent_streams as f64;

                        available_ratio < 1.0 - connection_saturation_threshold.clamp(0.0, 1.0)
                    },
                )
            };

            if should_expand && !pool_expanded {
                // Eagerly add new connection to the candidates since the pool
                // is running low on free h2 sessions.
                if let Some(candidate) = self.try_new_connection(has_some_closed, &mut inner) {
                    // no need to hold the lock any longer during the
                    // polling of the new candidate
                    drop(inner);

                    pool_expanded = true;
                    // if polling new connection failed, don't shadow other candidates
                    // that might be able to succeed.
                    match self.poll_new_candidate(cx, candidate) {
                        // Don't propagate the error if existing candidates
                        // (or the ones that has been added from the pool)
                        // haven't been polled yet. they may still succeed.
                        Poll::Ready(Err(_)) if !self.candidates.is_empty() => continue,
                        result => return result,
                    }
                }
            }

            if candidates_expanded {
                // Candidate list was expanded with existing pool connections and/or
                // a newly created connection. Restart the polling loop.
                //
                // Rotate by a random offset to roughly balance load
                // across connections. rotate_left is cheaper than shuffle.
                if self.candidates.len() > 1 {
                    let mid = rand::random_range(0..self.candidates.len());
                    self.candidates.rotate_left(mid);
                }

                drop(inner);
                continue;
            }

            // Fallback pool expansion, in case eager expansion was completely disabled.
            // Only hit this scenario when all candidates are pending and no more candidates to add.
            // Try to create a fresh connection if there are closed connections to evict or we're under the limit.
            if let Some(candidate) = self.try_new_connection(has_some_closed, &mut inner) {
                // no need to hold the lock any longer during the
                // polling of the new candidate
                drop(inner);

                return self.poll_new_candidate(cx, candidate);
            }

            // We only reach here when `!has_some_closed && len >= max`, so the
            // connection limit is saturated. All candidates are pending with
            // wakers registered — wait for any to free a stream.
            debug_assert!(inner.connections.len() >= self.config.max_connections.get());
            return Poll::Pending;
        }
    }

    /// Polls a new candidate connection for readiness. If not yet ready, queues it
    /// for later polling; if ready, select it and discards other candidates.
    fn poll_new_candidate(
        &mut self,
        cx: &mut Context<'_>,
        mut candidate: Connection<C>,
    ) -> Poll<Result<(), Error>> {
        match candidate.poll_ready(cx) {
            Poll::Pending => {
                // New connections should be immediately ready
                // Unless its all its permits has been already taken!
                // (or the inner connector is not ready)
                // This **should** not happen unless the pool is heavily
                // under contention.
                self.candidates.push(candidate);
                Poll::Pending
            }
            Poll::Ready(Ok(_)) => {
                self.ready = Some(candidate);
                self.candidates.clear();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }

    /// Try to add a new connection to the underlying shared pool.
    ///
    /// A new connection is attempted when either closed connections were detected
    /// (and need to be reaped) or the pool is below its max connection limit.
    ///
    /// The actual insertion upgrades to a write lock and rechecks the epoch to
    /// guard against concurrent modifications, so this may return `None` even
    /// when the preconditions appeared to hold.
    fn try_new_connection(
        &self,
        has_some_closed: bool,
        inner: &mut RwLockUpgradableReadGuard<'_, AuthorityPoolInner<C>>,
    ) -> Option<Connection<C>> {
        if has_some_closed || inner.connections.len() < self.config.max_connections.get() {
            // only try to open new connection if we still under the limit
            let epoch = inner.epoch;
            inner.with_upgraded(|inner| {
                inner.connections.retain(|c| !c.is_closed());

                if epoch != inner.epoch
                    || inner.connections.len() >= self.config.max_connections.get()
                {
                    // List of connections has been updated by a different thread,
                    // or expansion will cause number of connections to go above the limit.
                    return None;
                }

                inner.epoch = inner.epoch.wrapping_add(1);

                let candidate = Connection::new(self.connector.clone(), self.connection_config);
                inner.connections.push(candidate.clone());

                Some(candidate)
            })
        } else {
            None
        }
    }

    /// Sends a request over a connection selected by [`poll_ready`].
    ///
    /// # Panics
    /// Panics if called without a prior successful [`poll_ready`].
    pub fn call<B>(&mut self, request: http::Request<B>) -> ResponseFuture<B>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let mut conn = self
            .ready
            .take()
            .expect("poll_ready() was called until ready");
        conn.request(request)
    }
}

#[cfg(test)]
mod test {
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::task::Poll;

    use bytes::Bytes;
    use http::Request;
    use http_body_util::BodyExt;

    use crate::pool::conn::PermittedRecvStream;
    use crate::pool::test_util::TestConnector;

    use super::{AuthorityPool, PoolConfig};

    fn make_pool(
        max_concurrent_streams: u32,
        max_connections: usize,
    ) -> AuthorityPool<TestConnector> {
        AuthorityPool::new(
            TestConnector::new(max_concurrent_streams),
            PoolConfig {
                max_connections: NonZeroUsize::new(max_connections).unwrap(),
                initial_max_send_streams: NonZeroU32::new(max_concurrent_streams).unwrap(),
                ..Default::default()
            },
        )
    }

    async fn send_empty_request(pool: &mut AuthorityPool<TestConnector>) -> PermittedRecvStream {
        futures::future::poll_fn(|cx| pool.poll_ready(cx))
            .await
            .unwrap();
        let resp = pool
            .call(
                Request::builder()
                    .uri("http://test-host:80")
                    .body(http_body_util::Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.into_body()
    }

    /// First request creates a connection; the pool starts empty.
    #[tokio::test]
    async fn creates_connection_on_demand() {
        let mut pool = make_pool(10, 4);

        {
            let inner = pool.inner.read();
            assert_eq!(inner.connections.len(), 0);
        }

        let body = send_empty_request(&mut pool).await;

        {
            let inner = pool.inner.read();
            assert_eq!(inner.connections.len(), 1);
        }

        drop(body);
    }

    /// When all streams on existing connections are busy, a new connection is
    /// created (up to max_connections).
    #[tokio::test]
    async fn scales_up_when_streams_exhausted() {
        // 1 stream per connection, max 3 connections.
        let mut pool = make_pool(1, 3);

        // Hold response bodies to keep streams occupied.
        let b1 = send_empty_request(&mut pool).await;

        // Second request should trigger a second connection.
        let b2 = send_empty_request(&mut pool).await;

        {
            let inner = pool.inner.read();
            assert_eq!(inner.connections.len(), 2);
        }

        // Third request -> third connection.
        let b3 = send_empty_request(&mut pool).await;

        {
            let inner = pool.inner.read();
            assert_eq!(inner.connections.len(), 3);
        }

        drop((b1, b2, b3));
    }

    /// The pool does not create more connections than max_connections.
    /// When at capacity and all streams busy, poll_ready returns Pending.
    /// Dropping a held response body frees a stream and unblocks poll_ready.
    #[tokio::test]
    async fn respects_max_connections() {
        // 1 stream per connection, max 2 connections.
        let mut pool = make_pool(1, 2);

        let b1 = send_empty_request(&mut pool).await;
        let b2 = send_empty_request(&mut pool).await;

        {
            let inner = pool.inner.read();
            assert_eq!(inner.connections.len(), 2);
        }

        // Third poll_ready should return Pending (no capacity).
        let mut pool_clone = pool.clone();
        let result = futures::future::poll_fn(|cx| match pool_clone.poll_ready(cx) {
            Poll::Ready(r) => Poll::Ready(Some(r)),
            Poll::Pending => Poll::Ready(None),
        })
        .await;
        assert!(result.is_none(), "expected Pending when at max capacity");

        // Drop one body, freeing a stream.
        drop(b1);

        // Now poll_ready should succeed (wakers were registered on all connections).
        futures::future::poll_fn(|cx| pool_clone.poll_ready(cx))
            .await
            .unwrap();

        drop(b2);
    }

    /// Cloned pools share the same connection set.
    #[tokio::test]
    async fn clones_share_connections() {
        let pool = make_pool(10, 4);
        let mut p1 = pool.clone();
        let mut p2 = pool.clone();

        let _b1 = send_empty_request(&mut p1).await;

        // p2 should see the connection created by p1.
        {
            let inner = p2.inner.read();
            assert_eq!(inner.connections.len(), 1);
        }

        let _b2 = send_empty_request(&mut p2).await;

        // Still 1 connection (10 streams available, only 2 used).
        {
            let inner = p1.inner.read();
            assert_eq!(inner.connections.len(), 1);
        }
    }

    /// Concurrent requests with body echo work correctly through the pool.
    #[tokio::test]
    async fn concurrent_requests_with_echo() {
        let pool = make_pool(10, 4);
        let mut handles = tokio::task::JoinSet::default();

        for i in 0u8..200 {
            let mut p = pool.clone();
            handles.spawn(async move {
                futures::future::poll_fn(|cx| p.poll_ready(cx))
                    .await
                    .unwrap();
                let resp = p
                    .call(
                        Request::builder()
                            .uri("http://test-host:80")
                            .body(http_body_util::Full::new(Bytes::from(vec![i; 4])))
                            .unwrap(),
                    )
                    .await
                    .unwrap();

                let collected = resp.into_body().collect().await.unwrap().to_bytes();
                assert_eq!(
                    collected.as_ref(),
                    &[i; 4],
                    "response should echo request body"
                );
            });
        }

        handles.join_all().await;
    }
}
