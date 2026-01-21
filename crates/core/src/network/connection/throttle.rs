// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map;
use std::sync::LazyLock;
use std::time::Duration;

use ahash::HashMap;
use parking_lot::RwLock;
use restate_types::retries::{RetryIter, RetryPolicy};
use tokio::time::Instant;
use tracing::trace;

use crate::network::{ConnectError, Destination};

#[cfg(feature = "test-util")]
const MAX_DELAY: Duration = Duration::from_secs(1);
#[cfg(not(feature = "test-util"))]
const MAX_DELAY: Duration = Duration::from_secs(5);

const INITIAL_DELAY: Duration = Duration::from_millis(250);

static THROTTLE: LazyLock<ConnectThrottle> = LazyLock::new(ConnectThrottle::new);

struct State {
    down_until: Instant,
    retry_iter: RetryIter<'static>,
}

pub struct ConnectThrottle {
    destination_state: RwLock<HashMap<Destination, State>>,
}

impl ConnectThrottle {
    fn new() -> Self {
        Self {
            destination_state: RwLock::new(HashMap::default()),
        }
    }

    /// Checks if a connection to the given destination is allowed.
    pub fn may_connect(dest: &Destination) -> Result<(), ConnectError> {
        if let Some(state) = THROTTLE.destination_state.read().get(dest) {
            let now = Instant::now();
            let remaining = state.down_until.saturating_duration_since(now);
            if !remaining.is_zero() {
                // Immediately reject if we're still within the throttle window
                return Err(ConnectError::Throttled(remaining));
            }
        }
        Ok(())
    }

    /// Resets the connection throttle for the given destination.
    pub fn reset(dest: &Destination) {
        let mut last_failures = THROTTLE.destination_state.write();
        if last_failures.remove(dest).is_some() {
            trace!("Connection throttling to {} has been reset", dest);
        }
    }

    /// If the connection fails, record the time so we reject attempts for a while, and
    /// it resets the deadline to zero if connection was successful.
    pub fn note_connect_status(dest: &Destination, success: bool) {
        let mut last_failures = THROTTLE.destination_state.write();
        if success {
            if last_failures.remove(dest).is_some() {
                trace!(
                    "Connection succeeded to {}; resetting throttling window",
                    dest,
                );
            }
            return;
        }

        let now = Instant::now();
        match last_failures.entry(dest.clone()) {
            hash_map::Entry::Occupied(entry) => {
                let state = entry.into_mut();
                state.down_until = now + state.retry_iter.next().unwrap_or(MAX_DELAY);
                trace!(
                    "Connection failed to {}; re-setting throttling window to {:?}",
                    dest,
                    state.retry_iter.last_retry().unwrap(),
                );
            }
            hash_map::Entry::Vacant(entry) => {
                let mut retry_iter =
                    RetryPolicy::exponential(INITIAL_DELAY, 2.0, None, Some(MAX_DELAY)).into_iter();
                let down_until = now + retry_iter.next().unwrap_or(MAX_DELAY);
                let state = entry.insert(State {
                    down_until,
                    retry_iter,
                });
                trace!(
                    "connection failed to {}; setting throttling window to {:?}, next attempt in {:?}",
                    dest,
                    state.retry_iter.last_retry().unwrap_or(MAX_DELAY),
                    state.down_until.saturating_duration_since(now),
                );
            }
        }
    }
}
