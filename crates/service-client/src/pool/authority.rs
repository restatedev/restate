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

struct AuthorityPoolShared<C> {
    connector: C,
    config: PoolConfig,
    connection_config: ConnectionConfig,
    inner: Arc<RwLock<AuthorityPoolInner<C>>>,
    last_used: AtomicU64,
}

enum AuthorityPoolState<C> {
    ExpandCandidates {
        polled: Vec<Connection<C>>,
        unpolled: Vec<Connection<C>>,
    },
    PollCandidates {
        candidates: Vec<Connection<C>>,
    },
    Ready(Connection<C>),
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
    shared: Arc<AuthorityPoolShared<C>>,
    state: Option<AuthorityPoolState<C>>,
}

impl<C: Clone> Clone for AuthorityPool<C> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            state: None,
        }
    }
}

impl<C> AuthorityPool<C> {
    /// Updates the last-used timestamp to the current time.
    pub(crate) fn touch(&self) {
        self.shared
            .last_used
            .store(MillisSinceEpoch::now().as_u64(), Ordering::Relaxed);
    }

    /// Returns the last-used timestamp.
    pub(crate) fn last_used(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::from(self.shared.last_used.load(Ordering::Relaxed))
    }

    #[cfg(test)]
    pub(crate) fn set_last_used(&self, ts: MillisSinceEpoch) {
        self.shared.last_used.store(ts.as_u64(), Ordering::Relaxed);
    }

    /// Returns `true` if any connection has in-flight H2 streams.
    pub(crate) fn has_inflight(&self) -> bool {
        self.shared
            .inner
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

        let shared = AuthorityPoolShared {
            connector,
            config,
            connection_config,
            last_used: AtomicU64::new(MillisSinceEpoch::now().as_u64()),
            inner: Arc::new(RwLock::new(AuthorityPoolInner {
                epoch: 0,
                connections: Vec::new(),
            })),
        };

        Self {
            shared: Arc::new(shared),
            state: None,
        }
    }

    /// Polls the pool for a connection with an available H2 stream permit.
    ///
    /// Returns `Ready(Ok(()))` once a permit has been acquired on one connection,
    /// after which [`call`](Self::call) may be invoked exactly once.
    ///
    /// Internally driven by the [`AuthorityPoolState`] machine. Each iteration
    /// of the loop dispatches on the current state:
    ///
    /// - **`Ready`** — a connection was already selected by a previous poll.
    ///   Return `Ready(Ok(()))` immediately; the next [`call`](Self::call)
    ///   will consume it.
    /// - **`PollCandidates { candidates }`** — poll each candidate. Closed
    ///   candidates are evicted; the first to return `Ready(Ok)` wins and the
    ///   state transitions to `Ready`. Candidates that error are evicted; for
    ///   the ones that return `Pending` a waker has been registered with `cx`.
    ///   When the loop finishes without a winner, the surviving candidates
    ///   carry over into `ExpandCandidates` as `polled` so we can decide
    ///   whether to bring in more connections or expand.
    /// - **`ExpandCandidates { polled, unpolled }`** — walk the shared pool's
    ///   connection list, accumulating live-stream totals and pushing any
    ///   connection we haven't already seen onto `unpolled`. Then:
    ///   1. **Decide whether to expand.** The pool is allowed to grow when
    ///      there are no live connections at all (cold start), when every
    ///      live stream is currently in use (`total_available_streams == 0`),
    ///      or when the available-stream ratio falls below
    ///      `1.0 - connection_saturation_threshold`. The first two conditions
    ///      force expansion even when the threshold is `None`, so callers
    ///      cannot accidentally wedge themselves on full saturation by
    ///      disabling proactive expansion.
    ///   2. **If expansion was decided**, call [`Self::try_expand_pool`].
    ///      - On success, poll the new candidate. `Ready(Ok)` → transition
    ///        to `Ready`; `Pending` → push onto `polled`; `Ready(Err)`
    ///        propagates only if there is nothing else worth polling
    ///        (`unpolled` is empty), otherwise the failed candidate is
    ///        dropped and we continue with the rest.
    ///      - On race-loss (epoch bumped by another writer), re-enter
    ///        `ExpandCandidates` with the same `polled`/`unpolled`. The next
    ///        iteration picks up the connection added by the winning thread
    ///        and re-evaluates `scaleup`.
    ///   3. **If `unpolled` is non-empty**, transition to `PollCandidates`
    ///      with `polled ∪ unpolled` and loop back to poll them.
    ///   4. **Otherwise**, every reachable candidate is in `polled` (already
    ///      polled to `Pending` with a waker registered on `cx`) — return
    ///      `Poll::Pending`.
    ///
    /// **Back-pressure.** A new connection is created at most once per
    /// `ExpandCandidates` step, gated by the saturation check. Because a
    /// freshly-created (still-connecting) connection contributes its nominal
    /// `initial_max_send_streams` to both `available` and `max_concurrent`
    /// totals, a single in-flight connect drives the available-stream ratio
    /// near 1.0 and suppresses further expansion until callers actually start
    /// acquiring permits against it. This is the spiral guard against
    /// concurrent cold-start callers each opening their own connection.
    ///
    /// **Termination.** `Ready` and `PollCandidates` are one-shot transitions.
    /// `ExpandCandidates` either returns, transitions to `PollCandidates`, or
    /// re-enters itself on race-loss. The race-loss loop converges because
    /// every concurrent winner grows `inner.connections`, which we re-read on
    /// re-entry — so saturation eventually relaxes or we pick up the winner's
    /// connection into `unpolled`.
    ///
    /// **No deadlock.** When `Poll::Pending` is returned, at least one
    /// candidate had a waker registered on `cx`: the empty-candidates case
    /// would have driven `total_max == 0`, which forces expansion and either
    /// produces a candidate (waker registered via the new connection's
    /// `poll_ready`) or surfaces an error.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        loop {
            let state = self
                .state
                .take()
                .unwrap_or_else(|| AuthorityPoolState::ExpandCandidates {
                    polled: Vec::default(),
                    unpolled: Vec::default(),
                });

            match state {
                AuthorityPoolState::Ready(ready) => {
                    self.state = Some(AuthorityPoolState::Ready(ready));
                    return Poll::Ready(Ok(()));
                }
                AuthorityPoolState::PollCandidates { mut candidates } => {
                    let mut i = 0;
                    while i < candidates.len() {
                        let candidate = &mut candidates[i];
                        if candidate.is_closed() {
                            candidates.swap_remove(i);
                            continue;
                        }

                        match candidate.poll_ready(cx) {
                            Poll::Ready(Ok(_)) => {
                                self.state =
                                    Some(AuthorityPoolState::Ready(candidates.swap_remove(i)));
                                return Poll::Ready(Ok(()));
                            }
                            Poll::Ready(Err(err)) => {
                                // keep trying other candidates. If all candidates
                                // have been evicted, we try the remaining connections
                                // in the pool (after eviction).
                                candidates.swap_remove(i);
                                trace!("connection evicted from candidates due to error: {err}")
                            }
                            Poll::Pending => {
                                // Waker registered inside conn.poll_ready — we'll
                                // be woken when this connection frees a stream.
                                i += 1;
                            }
                        }
                    }

                    // none has finished or all has been evicted due to being closed.
                    // try to expand.
                    self.state = Some(AuthorityPoolState::ExpandCandidates {
                        polled: candidates,
                        unpolled: Vec::default(),
                    })
                }
                AuthorityPoolState::ExpandCandidates {
                    mut polled,
                    mut unpolled,
                } => {
                    let current_ids: HashSet<_> = polled
                        .iter()
                        .chain(unpolled.iter())
                        .map(|c| c.id())
                        .collect();

                    // try to expand the list of the candidates from the shared pool
                    let mut inner = self.shared.inner.upgradable_read();

                    let mut total_available_streams = 0usize;
                    let mut total_max_concurrent_streams = 0usize;

                    for candidate in &inner.connections {
                        if candidate.is_closed() {
                            continue;
                        }

                        total_available_streams =
                            total_available_streams.saturating_add(candidate.available_streams());
                        total_max_concurrent_streams = total_max_concurrent_streams
                            .saturating_add(candidate.max_concurrent_streams());

                        if current_ids.contains(&candidate.id()) {
                            continue;
                        }

                        unpolled.push(candidate.clone());
                    }

                    // We are allowed to create new connections and effectively expand
                    // the pool in the following conditions:
                    // - No more available streams (fully saturated or no connections)
                    // - Pool has zero streams (no connections)
                    // - available streams ratio (available/total) < 1.0 - saturation_threshold
                    let scaleup = total_available_streams == 0
                        || total_max_concurrent_streams == 0
                        || self
                            .shared
                            .config
                            .connection_saturation_threshold
                            .is_some_and(|connection_saturation_threshold| {
                                let available_ratio = total_available_streams as f64
                                    / total_max_concurrent_streams as f64;

                                available_ratio
                                    < 1.0 - connection_saturation_threshold.clamp(0.0, 1.0)
                            });

                    if scaleup {
                        match self.try_expand_pool(&mut inner) {
                            Some(mut candidate) => {
                                drop(inner);

                                match candidate.poll_ready(cx) {
                                    Poll::Ready(Ok(_)) => {
                                        self.state = Some(AuthorityPoolState::Ready(candidate));
                                        return Poll::Ready(Ok(()));
                                    }
                                    Poll::Pending => {
                                        // Add the connection to the list of polled candidates
                                        polled.push(candidate);
                                    }
                                    Poll::Ready(Err(err)) if unpolled.is_empty() => {
                                        return Poll::Ready(Err(err));
                                    }
                                    Poll::Ready(Err(_)) => {
                                        // there is a chance that one of the new candidates might succeed.
                                        // we can try to poll them again
                                    }
                                }
                            }
                            None => {
                                // another caller beat us to it. lets try again to pick up
                                // the new candidate
                                self.state =
                                    Some(AuthorityPoolState::ExpandCandidates { polled, unpolled });
                                continue;
                            }
                        }
                    }

                    // Here we can have two cases:
                    //   - `unpolled` is non-empty: we picked up connections
                    //     from the shared pool that haven't been polled yet —
                    //     transition to `PollCandidates` to drive them.
                    //   - `unpolled` is empty: every reachable candidate is
                    //     already in `polled` and was polled to `Pending`
                    //     with a waker registered on `cx`, so we just wait.
                    //
                    // The assert guards against an impossible empty state:
                    // `scaleup` would have been forced true if there are
                    // no candidates available.
                    assert!(!polled.is_empty() || !unpolled.is_empty());
                    if !unpolled.is_empty() {
                        polled.extend(unpolled);
                        self.state =
                            Some(AuthorityPoolState::PollCandidates { candidates: polled });
                    } else {
                        return Poll::Pending;
                    }
                }
            }
        }
    }

    /// Add a new connection to the underlying shared pool.
    ///
    /// Called only by [`Self::poll_ready`] after the saturation threshold has
    /// been crossed. Upgrades to a write lock, reaps closed connections, and
    /// pushes a new one. Returns `None` only if the epoch was bumped by a
    /// concurrent writer in between (defensive — concurrent writers are
    /// already serialized by the upgradable read lock).
    fn try_expand_pool(
        &self,
        inner: &mut RwLockUpgradableReadGuard<'_, AuthorityPoolInner<C>>,
    ) -> Option<Connection<C>> {
        let epoch = inner.epoch;
        inner.with_upgraded(|inner| {
            inner.connections.retain(|c| !c.is_closed());

            if epoch != inner.epoch {
                // List of connections has been updated by a different thread.
                return None;
            }

            inner.epoch = inner.epoch.wrapping_add(1);

            let candidate =
                Connection::new(self.shared.connector.clone(), self.shared.connection_config);
            inner.connections.push(candidate.clone());

            Some(candidate)
        })
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
        let Some(AuthorityPoolState::Ready(mut conn)) = self.state.take() else {
            panic!("expect poll_ready() was called until ready");
        };

        conn.request(request)
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroU32;

    use bytes::Bytes;
    use http::Request;
    use http_body_util::BodyExt;

    use crate::pool::conn::PermittedRecvStream;
    use crate::pool::test_util::TestConnector;

    use super::{AuthorityPool, PoolConfig};

    fn make_pool(max_concurrent_streams: u32) -> AuthorityPool<TestConnector> {
        AuthorityPool::new(
            TestConnector::new(max_concurrent_streams),
            PoolConfig {
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
        let mut pool = make_pool(10);

        {
            let inner = pool.shared.inner.read();
            assert_eq!(inner.connections.len(), 0);
        }

        let body = send_empty_request(&mut pool).await;

        {
            let inner = pool.shared.inner.read();
            assert_eq!(inner.connections.len(), 1);
        }

        drop(body);
    }

    /// When all streams on existing connections are busy, the pool grows
    /// without any cap. With 1 stream per connection, holding N response
    /// bodies forces N connections to exist.
    #[tokio::test]
    async fn scales_up_without_cap() {
        let mut pool = make_pool(1);

        let mut bodies = Vec::new();
        for expected_len in 1..=5 {
            bodies.push(send_empty_request(&mut pool).await);
            let inner = pool.shared.inner.read();
            assert_eq!(inner.connections.len(), expected_len);
        }

        drop(bodies);
    }

    /// Cloned pools share the same connection set.
    #[tokio::test]
    async fn clones_share_connections() {
        let pool = make_pool(10);
        let mut p1 = pool.clone();
        let mut p2 = pool.clone();

        let _b1 = send_empty_request(&mut p1).await;

        // p2 should see the connection created by p1.
        {
            let inner = p2.shared.inner.read();
            assert_eq!(inner.connections.len(), 1);
        }

        let _b2 = send_empty_request(&mut p2).await;

        // Still 1 connection (10 streams available, only 2 used).
        {
            let inner = p1.shared.inner.read();
            assert_eq!(inner.connections.len(), 1);
        }
    }

    /// Concurrent cold-start callers should not each open their own connection.
    /// With 10 streams per connection and 200 in-flight callers, the pool should
    /// open ~ceil(200/10) = 20 connections, not 200. The saturation-gated
    /// once-per-`poll_ready` rule provides the back-pressure.
    #[tokio::test]
    async fn back_pressure_under_concurrent_cold_start() {
        let streams_per_conn = 10u32;
        let concurrent_holds = 200usize;
        let pool = make_pool(streams_per_conn);

        let mut handles = tokio::task::JoinSet::default();
        for _ in 0..concurrent_holds {
            let mut p = pool.clone();
            handles.spawn(async move { send_empty_request(&mut p).await });
        }

        let bodies: Vec<_> = handles.join_all().await;

        let connections = pool.shared.inner.read().connections.len();
        let ideal = concurrent_holds.div_ceil(streams_per_conn as usize);
        // Allow some slack: the pool may temporarily race ahead while a
        // connection is still in CONNECTING and clones see the old totals.
        // The important invariant is "much less than `concurrent_holds`".
        assert!(
            connections <= ideal * 2,
            "expected at most {} connections, got {}",
            ideal * 2,
            connections,
        );
        assert!(
            connections >= ideal,
            "expected at least {} connections to satisfy {} concurrent holds with {} streams each, got {}",
            ideal,
            concurrent_holds,
            streams_per_conn,
            connections,
        );

        drop(bodies);
    }

    /// Concurrent requests with body echo work correctly through the pool.
    #[tokio::test]
    async fn concurrent_requests_with_echo() {
        let pool = make_pool(10);
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
