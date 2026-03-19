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
#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(
    pattern = "owned",
    build_fn(name = "build_inner", private),
    name = "PoolBuilder",
    default
)]
pub struct PoolConfig {
    /// Maximum number of connections to open to a single authority.
    pub(crate) max_connections: NonZeroUsize,

    /// Initial max H2 send streams per connection (passed to [`Connection::new`]).
    ///
    /// Most HTTP/2 frameworks default to 100 max-concurrent-streams. We use a
    /// lower initial value of 50 so the pool scales up sooner under load,
    /// limiting the number of requests queued behind a single pending connection.
    /// Once the connection is established, it discovers the remote peer's actual
    /// max-concurrent-streams and adjusts accordingly.
    #[builder(default = NonZeroU32::new(50).unwrap())]
    pub(crate) initial_max_send_streams: NonZeroU32,
    /// Maximum time to wait for an HTTP/2 PING response before declaring the
    /// connection dead and returning [`ConnectionError::KeepAliveTimeout`].
    /// Only meaningful when `keep_alive_interval` is `Some`. Defaults to 20 s.
    pub(crate) keep_alive_timeout: Duration,
    /// How often to send HTTP/2 PING frames to keep idle connections alive.
    /// `None` disables keep-alive pings entirely. Defaults to `None`.
    pub(crate) keep_alive_interval: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: NonZeroUsize::new(1).unwrap(),
            initial_max_send_streams: NonZeroU32::new(50).unwrap(),
            keep_alive_interval: None,
            keep_alive_timeout: Duration::from_secs(20),
        }
    }
}

impl PoolBuilder {
    pub fn build<C>(self, connector: C) -> super::Pool<C> {
        let config = self.build_inner().unwrap();
        super::Pool::new(connector, config)
    }
}
