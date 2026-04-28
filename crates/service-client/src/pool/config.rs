// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

/// Configuration for an [`AuthorityPool`].
#[derive(Debug, Clone, Copy, derive_builder::Builder)]
#[builder(
    pattern = "owned",
    build_fn(name = "build_inner", private),
    name = "PoolBuilder",
    default
)]
pub struct PoolConfig {
    /// Maximum number of connections to open to a single authority.
    ///
    /// Note: the number of connections per authority may temporarily exceed the
    /// configured limit during connection draining. This can happen when
    /// [`Self::idle_authority_timeout`] is reached and connections are being
    /// evicted, and a new request for the same authority arrives. In that case,
    /// a new connection may be initiated before the draining connections have
    /// fully closed.
    pub(crate) max_connections: NonZeroUsize,

    /// When available H2 stream capacity across all connections goes above
    /// this percentage, proactively open a new connection (if under
    /// `max_connections`). Set to None to disable. Defaults is 0.7
    #[builder(default = Some(0.7f64))]
    pub(crate) connection_saturation_threshold: Option<f64>,

    /// Initial max H2 send streams per connection (passed to [`Connection::new`]).
    ///
    /// Most HTTP/2 frameworks default to 100 max-concurrent-streams. We use a
    /// lower initial value of 50 so the pool scales up sooner under load,
    /// limiting the number of requests queued behind a single pending connection.
    /// Once the connection is established, it discovers the remote peer's actual
    /// max-concurrent-streams and adjusts accordingly.
    ///
    /// Default: 50
    #[builder(default = NonZeroU32::new(50).unwrap())]
    pub(crate) initial_max_send_streams: NonZeroU32,

    /// Upper bound on the per-connection max-send-streams.
    ///
    /// Once the HTTP/2 Connection Preface completes, `initial_max_send_streams`
    /// is replaced by the peer's `max_concurrent_streams` from its SETTINGS
    /// frame, or by `usize::MAX` if the peer advertises no value.
    ///
    /// We then clamp that effective limit to
    /// `min(peer.max_concurrent_streams, streams_per_connection_limit)` so the pool
    /// keeps opening new connections instead of funneling every request through
    /// a single one. Without this cap, a single connection can saturate its
    /// flow-control window and interacts poorly with load balancers, especially
    /// with end-to-end HTTP/2.
    ///
    /// Default: 128
    #[builder(default = NonZeroUsize::new(128).unwrap())]
    pub(crate) streams_per_connection_limit: NonZeroUsize,

    /// Maximum time to wait for an HTTP/2 PING response before declaring the
    /// connection dead and returning [`ConnectionError::KeepAliveTimeout`].
    /// Only meaningful when `keep_alive_interval` is `Some`. Defaults to 20 s.
    pub(crate) keep_alive_timeout: Duration,
    /// How often to send HTTP/2 PING frames to keep idle connections alive.
    /// `None` disables keep-alive pings entirely. Defaults to `None`.
    pub(crate) keep_alive_interval: Option<Duration>,
    /// How long an authority pool can be idle before it is evicted from the
    /// pool. `None` disables eviction entirely. Defaults to 5 minutes.
    pub(crate) idle_authority_timeout: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: NonZeroUsize::new(1).unwrap(),
            connection_saturation_threshold: Some(0.7f64),
            initial_max_send_streams: NonZeroU32::new(50).unwrap(),
            streams_per_connection_limit: NonZeroUsize::new(128).unwrap(),
            keep_alive_interval: None,
            keep_alive_timeout: Duration::from_secs(20),
            idle_authority_timeout: Some(Duration::from_secs(300)),
        }
    }
}

impl PoolBuilder {
    pub fn build<C: Clone + Send + Sync + 'static>(self, connector: C) -> super::Pool<C> {
        let config = self.build_inner().unwrap();
        super::Pool::new(connector, config)
    }
}
